//! End-to-end tests of the daemon binary over a real broker.
//!
//! Each case drives `flow-disk-daemon` exactly as it ships. The daemon runs in a
//! `sudo -n` child, because serving a device and mounting a filesystem need
//! `CAP_SYS_ADMIN` and cargo must not run as root. The test speaks the session gRPC
//! over the Unix socket. The mounts the daemon returns are root-owned, so file I/O
//! through them is privileged too, and it runs in `sudo -n` children of its own.
//!
//! A data-plane is expensive to start, so the cases share one, along with the
//! daemon. Each case works a journal of its own.

mod common;

use disk_daemon::BLOCK_SIZE;
use disk_daemon::proto;
use disk_daemon::{bitmap::Bitmap, chunk};
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

/// 128 MiB. `mkfs.ext4` accepts that size comfortably, and it keeps a case to a few
/// seconds.
const DEVICE_SIZE: u64 = 128 * 1024 * 1024;

/// How long a test waits for a teardown it cannot observe over the session.
const TEARDOWN: std::time::Duration = std::time::Duration::from_secs(30);

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
        fragment_root: data_plane.gazette.fragment_root.clone(),
        refresh_interval_seconds: 5 * 60,
    };
    let daemon = Daemon::start(&fixture, "shared").await;

    a_committed_disk_replays_into_an_identical_filesystem(&fixture, &daemon).await;
    an_unchanged_transaction_appends_nothing(&fixture, &daemon).await;
    a_broker_replacement_has_no_reply(&fixture, &daemon).await;
    a_disk_which_is_never_written_appends_nothing(&fixture, &daemon).await;
    a_mount_belongs_to_its_client(&fixture, &daemon).await;
    an_empty_suspended_journal_stays_suspended(&fixture, &daemon).await;
    a_suspended_journal_with_content_is_resumed_and_recovered(&fixture, &daemon).await;
    the_first_write_publishes_a_snapshot_of_the_formatted_image(&fixture, &daemon).await;
    an_absent_journal_is_terminal(&fixture, &daemon).await;
    an_unrecoverable_journal_is_terminal(&fixture, &daemon).await;
    a_recovered_ack_without_journal_content_is_terminal(&fixture, &daemon).await;
    a_recovered_ack_whose_records_were_destroyed_is_terminal(&fixture, &daemon).await;
    protocol_violations_are_terminal(&fixture, &daemon).await;
    a_committed_disk_reopens_with_its_contents(&fixture, &daemon).await;
    an_acknowledgement_lost_after_commit_is_repaired(&fixture, &daemon).await;
    an_uncommitted_delta_is_discarded(&fixture, &daemon).await;
    an_orphaned_first_use_yields_a_fresh_disk(&fixture, &daemon).await;
    a_floor_hint_only_seeks_a_replay(&fixture, &daemon).await;
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

/// Horizons need a daemon of their own, and a data-plane of its own with it. The
/// shipped thresholds open a horizon only after a gigabyte of journal, and these
/// cases are all about what happens once one is open. The thresholds set below are
/// tiny, but they are the same flags an operator sets.
///
/// The brokers also re-list these journals every second. A fragment deleted from
/// the store is then one no broker can still serve from a local spool file.
#[tokio::test]
async fn disk_daemon_horizon_tests() {
    common::check_prerequisites();

    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");

    let fixture = Fixture {
        dir: tempfile::tempdir().expect("tempdir"),
        endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
        credential: credential(&data_plane.gazette),
        client: data_plane.journal_client.clone(),
        fragment_root: data_plane.gazette.fragment_root.clone(),
        refresh_interval_seconds: 1,
    };
    let daemon = Daemon::start_with(
        &fixture,
        "horizons",
        &[
            "--horizon-open-ratio",
            "0.1",
            "--horizon-copy-ratio",
            "1.0",
            "--horizon-minimum-bytes",
            "1048576",
        ],
    )
    .await;

    a_horizon_bounds_what_recovery_reads(&fixture, &daemon).await;
    a_replacement_session_resumes_an_open_horizon(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.assert_no_leaks();

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// Sustained traffic opens and completes horizons, and each commit which completes
/// one reports its floor. Everything below the floor the client has kept is then
/// deleted from the fragment store, and the disk still recovers from what remains.
async fn a_horizon_bounds_what_recovery_reads(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/horizons";
    let source = fixture.dir.path().join("horizons");
    write_source(&source, 1);

    // A first session, short enough that no horizon of it can complete. The
    // recovery at the end must not need its fragments.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();

    () = session.commit(ack).await.unwrap();
    assert_eq!(session.floor(), 0, "nothing completed yet");
    () = session.close().await;

    // A session stamps its records with the wall clock. This sleep therefore puts
    // the floor derived below after the fragments written above.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    for _ in 0..CHURN_DELTAS {
        () = churn(&mut session, &mount).await;

        if session.floor() != 0 {
            break;
        }
    }
    let floor = session.floor();
    assert!(floor != 0, "no horizon of {journal} completed");
    () = session.close().await;

    // Long enough that the brokers have re-listed the store. What is deleted here
    // is therefore content they no longer hold open locally either.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let deleted = fixture.delete_fragments_before(journal, floor_seconds(floor));

    assert!(deleted > 0, "no fragment of {journal} was below {floor}");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // What a replay must read is now bounded by the disk's own size rather than
    // by its history, and what remains rebuilds the filesystem on its own.
    let image = fixture.dir.path().join("horizons.img");
    _ = fixture.replay(journal, &image).await;
    fixture.assert_content_matches(&image, &source);

    let (allocated, retained) = (
        allocated_bytes(&image),
        fixture.retained_range(journal).await,
    );
    assert!(
        retained < 8 * allocated,
        "{retained} bytes of journal are retained for a disk holding {allocated}",
    );

    // The client hands its floor back, which seeks this recovery past the
    // fragments which are gone.
    let mut session = daemon.session().await;
    let mount = session
        .open(proto::Open {
            floor_hint: floor,
            ..fixture.provisioned(journal).await
        })
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));

    // The recovery derives the floor again from the journal itself, so the value
    // the client holds only ever advances across a disk's whole history.
    let later = session.floor();
    assert!(
        later >= floor,
        "the floor moved from {floor} back to {later}"
    );
    () = session.close().await;
}

/// A horizon belongs to its disk rather than to the session which opened it. A
/// replacement resumes the horizon it finds open. The floor it goes on to derive is
/// the position that earlier session chose.
async fn a_replacement_session_resumes_an_open_horizon(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/resumed";
    let source = fixture.dir.path().join("resumed");
    write_source(&source, 1);

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();

    // Run traffic until a horizon completes, then one delta more. That delta opens
    // the horizon this session is killed in the middle of. One delta cannot
    // discharge a horizon of this disk, and the floor holding still says so.
    for _ in 0..CHURN_DELTAS {
        () = churn(&mut session, &mount).await;

        if session.floor() != 0 {
            break;
        }
    }
    let completed = session.floor();
    assert!(completed != 0, "no horizon of {journal} completed");

    () = churn(&mut session, &mount).await;
    assert_eq!(
        session.floor(),
        completed,
        "the delta before the kill completed its horizon, leaving none open",
    );

    drop(session);
    fixture.wait_for_teardown().await;

    // A horizon the replacement restarted, rather than resumed, would open at a
    // record appended after this moment.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let replaced_at = unix_seconds();

    let mut session = daemon.session().await;
    let mount = session
        .open(proto::Open {
            floor_hint: completed,
            ..fixture.provisioned(journal).await
        })
        .await
        .unwrap();

    // The replay of that range derives the floor its predecessor established.
    assert_eq!(session.floor(), completed, "the recovery derived the floor");

    let mut resumed = 0;
    for _ in 0..CHURN_DELTAS {
        () = churn(&mut session, &mount).await;

        if session.floor() != completed {
            resumed = session.floor();
            break;
        }
    }
    assert!(resumed != 0, "the resumed horizon never completed");

    assert!(
        floor_seconds(resumed) < replaced_at,
        "the floor {resumed} is a horizon the replacement session opened for itself",
    );
    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;
}

/// Disks a soak works at once, and the rounds of traffic it puts through each.
const SOAK_DISKS: usize = 6;
const SOAK_ROUNDS: usize = 4;

/// Many disks at once under mixed traffic. Each round, a share of them lose the
/// delta they prepared and then recover it.
///
/// It asserts that a long run leaves nothing behind. Every disk holds exactly the
/// generation it last committed. No thread, descriptor, device, or mount outlives
/// the session which made it. It also measures the [`Cost`] of a host of disks.
#[tokio::test]
async fn disk_daemon_soak_test() {
    common::check_prerequisites();

    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");

    let fixture = Fixture {
        dir: tempfile::tempdir().expect("tempdir"),
        endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
        credential: credential(&data_plane.gazette),
        client: data_plane.journal_client.clone(),
        fragment_root: data_plane.gazette.fragment_root.clone(),
        refresh_interval_seconds: 5 * 60,
    };
    let daemon = Daemon::start(&fixture, "soak").await;

    // One tree per generation. A disk which lost its delta is compared against the
    // generation it last committed, and not against the newest one.
    let generations: Vec<std::path::PathBuf> = (0..=SOAK_ROUNDS)
        .map(|generation| {
            let path = fixture.dir.path().join(format!("soak-{generation}"));
            write_source(&path, generation as u8 + 1);
            path
        })
        .collect();

    let idle = daemon.cost();
    let mut disks = Vec::new();

    for index in 0..SOAK_DISKS {
        let journal = format!("acmeCo/disk/soak-{index}");
        let mut session = daemon.session().await;
        let mount = session
            .open(fixture.provisioned(&journal).await)
            .await
            .unwrap();

        copy_through(&generations[0], &mount);
        let ack = session.prepare().await.unwrap();
        () = session.commit(ack).await.unwrap();

        disks.push(SoakDisk {
            journal,
            mount,
            session: Some(session),
            committed: 0,
        });
    }
    let busy = daemon.cost();
    let mut killed = 0;

    for round in 1..=SOAK_ROUNDS {
        let load = disks
            .iter()
            .map(|disk| soak_load(&disk.mount, &generations[round], round));

        for outcome in futures::future::join_all(load).await {
            () = outcome.expect("a round of soak traffic");
        }

        for (index, disk) in disks.iter_mut().enumerate() {
            let session = disk.session.as_mut().expect("a live session");
            let ack = session.prepare().await.unwrap();

            assert!(!ack.is_empty(), "{} changed in round {round}", disk.journal);

            // A share of the disks lose the delta they just prepared. That is the
            // crash this soak varies over.
            if roll(index, round).is_multiple_of(3) {
                _ = disk.session.take();
                killed += 1;
            } else {
                () = session.commit(ack).await.unwrap();
                disk.committed = round;
            }
        }

        for disk in disks.iter_mut().filter(|disk| disk.session.is_none()) {
            () = wait_unmounted(&disk.mount).await;

            let mut session = daemon.session().await;
            let mount = session
                .open(fixture.provisioned(&disk.journal).await)
                .await
                .unwrap();

            assert_tree_matches(&generations[disk.committed], &format!("{mount}/data"));

            disk.mount = mount;
            disk.session = Some(session);
        }
    }
    assert!(killed > 0, "no disk lost a delta over {SOAK_ROUNDS} rounds");

    // Every disk holds what it last committed, whatever happened to it.
    for disk in disks.iter() {
        assert_tree_matches(
            &generations[disk.committed],
            &format!("{}/data", disk.mount),
        );
    }
    let mounts: Vec<String> = disks.iter().map(|disk| disk.mount.clone()).collect();

    for disk in disks.iter_mut() {
        () = disk.session.take().expect("a live session").close().await;
    }
    for mount in mounts {
        () = wait_unmounted(&mount).await;
    }
    let ended = daemon.cost();

    eprintln!("soak cost: idle {idle:?}, serving {SOAK_DISKS} disks {busy:?}, ended {ended:?}");

    // An owner thread per disk, and a shared pool of kernel workers.
    assert_eq!(busy.owners, SOAK_DISKS, "{busy:?}");
    assert!(
        busy.workers > 0,
        "no io_uring worker served the disks: {busy:?}"
    );
    assert!(
        busy.threads >= idle.threads + SOAK_DISKS,
        "{busy:?} against an idle {idle:?}",
    );

    // Nothing a disk held outlives it. The descriptors allowed for here are the
    // broker connections the router holds until its next sweep.
    assert_eq!(ended.owners, 0, "{ended:?}");
    assert!(
        ended.files <= idle.files + 8,
        "{ended:?} against an idle {idle:?}",
    );

    daemon.drain().await;
    fixture.assert_no_leaks();

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// One disk of a soak run, and the generation it last committed.
struct SoakDisk {
    journal: String,
    mount: String,
    /// Taken when the disk loses a delta. The session which recovers that disk
    /// replaces it.
    session: Option<Session>,
    committed: usize,
}

/// One round of mixed traffic on a mounted disk. It copies a generation of files in,
/// writes scratch, deletes the scratch of the round before so that the filesystem
/// discards, and reads a file back.
///
/// It never blocks the runtime, so the soak can work every disk at once.
async fn soak_load(mount: &str, source: &std::path::Path, round: usize) -> anyhow::Result<()> {
    () = sudo_async(&["cp", "-rT", path(source), &format!("{mount}/data")]).await?;
    () = sudo_async(&[
        "dd",
        "if=/dev/urandom",
        "bs=64k",
        "count=64",
        "status=none",
        &format!("of={mount}/scratch-{round}"),
    ])
    .await?;
    () = sudo_async(&["cp", &format!("{mount}/data/large"), "/dev/null"]).await?;
    () = sudo_async(&["rm", "-f", &format!("{mount}/scratch-{}", round - 1)]).await?;

    Ok(())
}

/// A deterministic roll. A soak run is therefore reproducible, and the disks it
/// kills still vary between rounds.
fn roll(index: usize, round: usize) -> u64 {
    let mut state = 0x9e3779b97f4a7c15 ^ ((index as u64) << 32) ^ round as u64;

    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;

    state
}

/// Wait for parked writes to complete. This does not hold the runtime they must
/// complete on.
async fn wait_for_exit(writer: &mut std::process::Child) {
    let deadline = std::time::Instant::now() + TEARDOWN;

    while std::time::Instant::now() < deadline {
        match writer.try_wait().expect("polling a writer") {
            Some(status) => return assert!(status.success(), "a parked write failed: {status}"),
            None => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    _ = writer.kill();
    panic!("parked writes did not complete once the broker returned");
}

/// Wait for the disk of a dropped session to be torn down. Its client cannot
/// observe that teardown, because its stream is already gone.
async fn wait_unmounted(mount: &str) {
    let deadline = std::time::Instant::now() + TEARDOWN;

    while std::time::Instant::now() < deadline {
        if !is_mounted(mount) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{mount} was not torn down");
}

/// Run a privileged command without blocking the runtime.
async fn sudo_async(args: &[&str]) -> anyhow::Result<()> {
    let mut command = async_process::Command::new("sudo");
    command.arg("-n").args(args);

    let output = async_process::output(&mut command).await?;

    anyhow::ensure!(
        output.status.success(),
        "sudo {args:?} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

/// Faults a daemon must survive. These are a broker it cannot reach, a credential
/// which runs out under it, a session which takes its journal away, and a SIGTERM
/// while a disk is being written.
#[tokio::test]
async fn disk_daemon_fault_tests() {
    common::check_prerequisites();

    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");

    let fixture = Fixture {
        dir: tempfile::tempdir().expect("tempdir"),
        endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
        credential: credential(&data_plane.gazette),
        client: data_plane.journal_client.clone(),
        fragment_root: data_plane.gazette.fragment_root.clone(),
        refresh_interval_seconds: 5 * 60,
    };
    let daemon = Daemon::start(&fixture, "faults").await;

    a_broker_outage_stalls_writes_and_then_resumes(&fixture, &daemon).await;
    a_credential_is_replaced_before_it_expires(&fixture, &data_plane, &daemon).await;
    a_replacement_session_fences_the_first(&fixture, &daemon).await;
    the_crate_client_drives_a_session(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.assert_no_leaks();

    a_sigterm_under_load_tears_every_disk_down(&fixture).await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// A broker which cannot be reached parks the device rather than failing it. Appends
/// retry, the capture channel fills, and writes wait. Naming a reachable broker
/// again releases those writes, and the delta commits as though nothing happened.
async fn a_broker_outage_stalls_writes_and_then_resumes(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/outage";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.source("outage");

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();

    // A socket nothing is listening on. Dialing it fails transiently, exactly as a
    // broker restart looks from here.
    () = session
        .send(proto::request::Request::Broker(proto::Broker {
            endpoint: format!(
                "unix://localhost{}/absent.sock",
                fixture.dir.path().display()
            ),
            credential: fixture.credential.clone(),
        }))
        .await;

    // This is more than the capture channel can hold. The device parks once the
    // writer stops taking from that channel.
    let mut writer = std::process::Command::new("sudo")
        .args([
            "-n",
            "dd",
            "if=/dev/urandom",
            "bs=1M",
            "count=48",
            "conv=fsync",
        ])
        .arg(format!("of={mount}/stalled"))
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    assert!(
        writer.try_wait().expect("polling dd").is_none(),
        "the disk kept taking writes while its broker was unreachable",
    );

    // The replacement does not go through the writer. That writer is retrying
    // against the broker being replaced.
    () = session
        .send(proto::request::Request::Broker(proto::Broker {
            endpoint: fixture.endpoint.clone(),
            credential: fixture.credential.clone(),
        }))
        .await;

    () = wait_for_exit(&mut writer).await;
    let ack = session.prepare().await.unwrap();

    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    // What was committed across the outage is what recovers.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    assert_eq!(
        sudo(&["stat", "-c", "%s", &format!("{mount}/stalled")]).trim(),
        (48 * 1024 * 1024).to_string(),
    );
    () = session.close().await;
}

/// A session outlives the credential it opened with, because its client replaces
/// that credential while it is still valid.
///
/// Replacement before expiry is the contract, per the crate README. This does not
/// assert the reverse, because the reverse cannot be made deterministic. Whether an
/// append falls inside the expiry window is the filesystem's decision, not the
/// test's.
async fn a_credential_is_replaced_before_it_expires(
    fixture: &Fixture,
    data_plane: &e2e_support::DataPlane,
    daemon: &Daemon,
) {
    const LIFETIME: u64 = 8;

    let journal = "acmeCo/disk/refreshed-credential";

    let open = proto::Open {
        broker: Some(proto::Broker {
            endpoint: fixture.endpoint.clone(),
            credential: credential_lasting(&data_plane.gazette, LIFETIME),
        }),
        ..fixture.provisioned(journal).await
    };
    let mut session = daemon.session().await;
    let mount = session.open(open.clone()).await.unwrap();

    let first = fixture.source("refreshed-credential");
    copy_through(&first, &mount);

    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();

    // Replaced while the credential it replaces is still good, as a client
    // holding a short-lived token is required to do.
    () = session
        .send(proto::request::Request::Broker(proto::Broker {
            endpoint: fixture.endpoint.clone(),
            credential: fixture.credential.clone(),
        }))
        .await;

    // Long enough that the credential this session opened with has run out.
    // Everything below is therefore the replacement's doing.
    tokio::time::sleep(std::time::Duration::from_secs(LIFETIME + 2)).await;

    let second = fixture.dir.path().join("refreshed-credential-2");
    write_source(&second, 2);
    copy_through(&second, &mount);

    let ack = session.prepare().await.unwrap();
    assert!(!ack.is_empty(), "the second generation changed the disk");

    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    // What the journal holds is what the replacement appended.
    let image = fixture.dir.path().join("refreshed-credential.img");
    _ = fixture.replay(journal, &image).await;
    fixture.assert_content_matches(&image, &second);

    // The credential it replaced really had expired. A session which opens with
    // that credential cannot even probe.
    let mut session = daemon.session().await;
    let status = session.open(open).await.unwrap_err();

    assert!(status.message().contains("probing"), "{status}");
    () = session.ended().await;
}

/// Two sessions of one journal. The second claims the author register, and the first
/// learns of that on its next append. The loser is terminal and says so with
/// `ABORTED`. The winner recovers everything the loser committed.
async fn a_replacement_session_fences_the_first(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/fenced";
    let source = fixture.source("fenced");

    let mut loser = daemon.session().await;
    let mount = loser
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    copy_through(&source, &mount);
    let ack = loser.prepare().await.unwrap();
    () = loser.commit(ack).await.unwrap();

    // The journal now holds committed state. This session therefore claims the
    // fence as it opens, rather than deferring it to a first append.
    let mut winner = daemon.session().await;
    let recovered = winner
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{recovered}/data"));

    // The loser's next delta is refused by the broker's register check.
    let displaced = fixture.dir.path().join("fenced-2");
    write_source(&displaced, 2);
    copy_through(&displaced, &mount);

    let status = loser.prepare().await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::Aborted, "{status}");
    assert!(status.message().contains("RegisterMismatch"), "{status}");

    () = loser.ended().await;
    () = winner.close().await;
}

/// `client::Disk` drives a disk end to end, and the next stream opens what that
/// client committed.
///
/// This is the only case here which uses the crate's own client. The others speak
/// the protocol directly, because they assert what the daemon does with requests
/// that client cannot express.
async fn the_crate_client_drives_a_session(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/by-hand";
    let source = fixture.source("by-hand");

    let disks = disk_daemon::client::Client::connect(&daemon.uds_path)
        .await
        .unwrap();

    let (mut client, mount, floor) = disks
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_eq!(floor, None, "a fresh disk derives no floor");

    copy_through(&source, path(&mount));

    let ack = client
        .prepare()
        .await
        .unwrap()
        .expect("the copy changed the disk");

    // A short session completes no horizon, so it establishes no floor either.
    assert_eq!(client.commit(ack).await.unwrap(), None);

    assert!(
        client.prepare().await.unwrap().is_none(),
        "nothing changed since the commit",
    );
    () = client.close().await.unwrap();

    let mut session = daemon.session().await;
    let reopened = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{reopened}/data"));
    () = session.close().await;
}

/// A daemon signalled while a disk is being written ends its sessions. It tears down
/// what they held, then exits cleanly.
async fn a_sigterm_under_load_tears_every_disk_down(fixture: &Fixture) {
    let daemon = Daemon::start(fixture, "sigterm").await;
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned("acmeCo/disk/sigterm").await)
        .await
        .unwrap();

    let mut writer = std::process::Command::new("sudo")
        .args(["-n", "dd", "if=/dev/urandom", "bs=1M", "count=64"])
        .arg(format!("of={mount}/load"))
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    () = daemon.drain().await;

    _ = writer.wait().expect("waiting for dd");
    () = session.ended().await;

    fixture.assert_no_leaks();
}

/// Deltas a horizon case runs before it gives up on one completing. This is generous
/// against the handful its thresholds need.
const CHURN_DELTAS: usize = 20;

/// Rewrite a megabyte of the disk and commit it. This is one delta of ordinary
/// traffic. It earns copy budget without discharging much of a horizon.
async fn churn(session: &mut Session, mount: &str) {
    _ = sudo(&[
        "dd",
        "if=/dev/urandom",
        "bs=1M",
        "count=1",
        "conv=fsync",
        &format!("of={mount}/churn"),
    ]);

    let ack = session.prepare().await.unwrap();
    assert!(!ack.is_empty(), "the rewrite changed the disk");

    () = session.commit(ack).await.unwrap();
}

/// Wall-clock second of a recovery floor. A replay which begins there seeks its
/// journal's fragments by this second.
fn floor_seconds(floor: u64) -> u64 {
    uuid::Clock::from_u64(floor).to_unix().0
}

/// Bytes the host filesystem allocated to `image`, which is a disk's true footprint.
/// `st_blocks` counts what the image holds, and not the sparse size it presents.
fn allocated_bytes(image: &std::path::Path) -> i64 {
    std::os::unix::fs::MetadataExt::blocks(&std::fs::metadata(image).expect("an image")) as i64
        * 512
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A replay of the journal reproduces exactly the files which were written through
/// the mount and committed, over several sequential transactions.
async fn a_committed_disk_replays_into_an_identical_filesystem(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/committed";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.dir.path().join("source");

    for generation in 1..=3u8 {
        write_source(&source, generation);
        copy_through(&source, &mount);

        let ack = session.prepare().await.unwrap();
        assert!(!ack.is_empty(), "generation {generation} changed the disk");

        () = session.commit(ack).await.unwrap();
    }
    () = session.close().await;

    let image = fixture.dir.path().join("replay.img");
    let covered = fixture.replay(journal, &image).await;

    assert!(covered > 0, "the journal replayed no blocks");
    fixture.assert_content_matches(&image, &source);
}

/// A transaction which changed nothing prepares no acknowledgement. It also appends
/// nothing to the journal.
async fn an_unchanged_transaction_appends_nothing(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/unchanged";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    copy_through(&fixture.source("once"), &mount);

    let ack = session.prepare().await.unwrap();
    assert!(!ack.is_empty());
    () = session.commit(ack).await.unwrap();

    let head = fixture.head(journal).await;

    // Nothing touches the disk between the two publications. The second therefore
    // finds no delta at all, and the journal does not move.
    assert!(session.prepare().await.unwrap().is_empty());
    assert_eq!(fixture.head(journal).await, head);

    () = session.close().await;
}

/// A broker replacement has no reply. The reply to the next request belongs to that
/// next request.
async fn a_broker_replacement_has_no_reply(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/refreshed";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    copy_through(&fixture.source("once"), &mount);

    session
        .send(proto::request::Request::Broker(proto::Broker {
            endpoint: fixture.endpoint.clone(),
            credential: fixture.credential.clone(),
        }))
        .await;

    let ack = session.prepare().await.unwrap();
    assert!(!ack.is_empty());

    () = session.commit(ack).await.unwrap();
    () = session.close().await;
}

/// A disk which is formatted and mounted but never written appends nothing at all.
/// It carries no information, because formatting it again reproduces it.
///
/// Nothing lands, not even a fence, so its journal is exactly as its caller made
/// it. That is what lets a data plane leave an unused disk suspended.
async fn a_disk_which_is_never_written_appends_nothing(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/untouched";

    for _ in 0..2 {
        let mut session = daemon.session().await;

        _ = session
            .open(fixture.provisioned(journal).await)
            .await
            .unwrap();
        assert!(session.prepare().await.unwrap().is_empty());

        () = session.close().await;

        assert_eq!(fixture.head(journal).await, 0, "{journal} was appended to");
        assert_eq!(fixture.author(journal).await, None, "{journal} was fenced");
    }
}

/// The mount a session returns belongs to the client which opened it, so a client
/// needs no privilege of its own.
///
/// The daemon runs as root, and `mkfs` leaves the root directory of a new filesystem
/// to root. A session therefore gives that directory to the peer of its stream,
/// which here is this test. Every file operation below runs unprivileged, unlike the
/// rest of this suite.
async fn a_mount_belongs_to_its_client(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/unprivileged";
    let content = b"written by an unprivileged client";

    let ours = std::fs::metadata(fixture.dir.path()).expect("the test's own directory");
    let ours = (
        std::os::unix::fs::MetadataExt::uid(&ours),
        std::os::unix::fs::MetadataExt::gid(&ours),
    );
    assert_ne!(ours.0, 0, "this suite must not run as root");

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    let held = std::fs::metadata(&mount).expect("the mount");
    let held = (
        std::os::unix::fs::MetadataExt::uid(&held),
        std::os::unix::fs::MetadataExt::gid(&held),
    );
    assert_eq!(held, ours, "{mount} does not belong to this test");

    () = std::fs::write(format!("{mount}/unprivileged"), content).expect("writing the mount");

    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    // A recovery rebuilds the image block for block, so the root directory it
    // replays already belongs to this client and nothing changes it again.
    let mut session = daemon.session().await;
    let recovered = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_eq!(
        std::fs::read(format!("{recovered}/unprivileged")).expect("reading the mount"),
        content,
    );
    () = session.close().await;
}

/// An unused journal Gazette has fully suspended is served from its listing alone.
/// The whole session — open, an empty publication, and close — never touches the
/// journal's append path, so it stays suspended throughout.
async fn an_empty_suspended_journal_stays_suspended(fixture: &Fixture, daemon: &Daemon) {
    use broker::journal_spec::suspend::Level;

    let journal = "acmeCo/disk/suspended-empty";
    let open = fixture.provisioned(journal).await;

    () = fixture
        .suspend(journal, broker::append_request::Suspend::IfFlushed)
        .await;

    let suspend = fixture.suspension(journal).await.expect("suspended");
    assert_eq!(suspend.level, Level::Full as i32);
    assert_eq!(suspend.offset, 0);

    let mut session = daemon.session().await;
    let mount = session.open(open).await.unwrap();

    assert_eq!(sudo(&["ls", "-A", &mount]).trim(), "lost+found");
    assert!(session.prepare().await.unwrap().is_empty());
    () = session.close().await;

    let suspend = fixture.suspension(journal).await.expect("still suspended");
    assert_eq!(
        suspend.level,
        Level::Full as i32,
        "the disk woke its journal"
    );
}

/// A journal suspended over committed content cannot be proved empty, so the next
/// open wakes it deliberately and recovers the disk from it.
async fn a_suspended_journal_with_content_is_resumed_and_recovered(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    use broker::journal_spec::suspend::Level;

    let journal = "acmeCo/disk/suspended-content";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.source("resumed");

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();

    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    // NOW suspends without waiting for an idle flush interval. The journal holds
    // content, so this is a partial suspension at the committed head.
    () = fixture
        .suspend(journal, broker::append_request::Suspend::Now)
        .await;

    let suspend = fixture.suspension(journal).await.expect("suspended");
    assert_ne!(suspend.level, Level::None as i32);
    assert!(suspend.offset > 0);

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));

    let suspend = fixture
        .suspension(journal)
        .await
        .expect("a suspension record");
    assert_eq!(suspend.level, Level::None as i32, "the open resumed it");
    () = session.close().await;
}

/// The first mutation after mount publishes a snapshot of the image. The first delta
/// therefore holds the whole formatted filesystem and nothing more. The ranges a
/// prezeroed format left as holes are holes in a replay of it too.
async fn the_first_write_publishes_a_snapshot_of_the_formatted_image(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    let journal = "acmeCo/disk/first-write";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.source("tiny");

    copy_through(&source, &mount);

    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    let image = fixture.dir.path().join("first-write.img");
    let covered = fixture.replay(journal, &image).await;

    // A few small files could not account for a filesystem's metadata. The replay
    // also mounts, which only the snapshot makes possible.
    assert!(
        covered > 512,
        "the first delta covered only {covered} blocks"
    );
    let allocated = allocated_bytes(&image) as u64;

    assert!(
        allocated < DEVICE_SIZE / 4,
        "the replayed image allocated {allocated} of the device's {DEVICE_SIZE} bytes",
    );
    fixture.assert_content_matches(&image, &source);
}

/// A journal which does not exist is terminal, before any device exists. The
/// daemon creates none, so its caller must have made one first.
async fn an_absent_journal_is_terminal(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/absent";
    let mut session = daemon.session().await;

    let status = session.open(fixture.open(journal)).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert!(status.message().contains("does not exist"), "{status}");
    () = session.ended().await;

    // The daemon looked, and did not make one of its own.
    assert!(list(&fixture.client, journal).await.journals.is_empty());
}

/// A journal whose spec a disk could not be recovered from is terminal. The
/// daemon validates the spec its caller applied, and refuses rather than fixes
/// one, because the spec belongs to that caller.
async fn an_unrecoverable_journal_is_terminal(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/unrecoverable";

    // Age-based deletion cannot see the recovery floor, so it can remove records
    // a live disk still needs.
    let mut spec = fixture.spec(journal);
    spec.fragment.as_mut().unwrap().retention = Some(std::time::Duration::from_secs(86400).into());

    () = common::create_journal(&fixture.client, spec).await.unwrap();

    let mut session = daemon.session().await;
    let status = session.open(fixture.open(journal)).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert!(status.message().contains("fragment retention"), "{status}");
    () = session.ended().await;
}

/// A recovered acknowledgement names committed state, so a journal which holds
/// no content at all was emptied out from under it. That is terminal rather than
/// a fresh disk which hides the loss.
async fn a_recovered_ack_without_journal_content_is_terminal(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/deleted";
    let mut session = daemon.session().await;

    let open = proto::Open {
        recovered_acks: vec![bytes::Bytes::from_static(b"a committed acknowledgement")],
        ..fixture.provisioned(journal).await
    };
    let status = session.open(open).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert!(status.message().contains("is empty"), "{status}");
    () = session.ended().await;

    // The failure precedes any claim, so nothing reached the journal.
    assert_eq!(fixture.head(journal).await, 0);
}

/// A recovered acknowledgement proves a broker confirmed the data appends of its
/// delta. A journal whose replay applies nothing lost those records even though
/// its head survived them, exactly as an emptied journal did. It is terminal
/// rather than a fresh disk which hides the loss.
async fn a_recovered_ack_whose_records_were_destroyed_is_terminal(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    // A prepared-but-uncommitted first use leaves a journal with content and no
    // committed state, so its replay applies nothing.
    let journal = "acmeCo/disk/destroyed";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    copy_through(&fixture.source("destroyed"), &mount);

    assert!(!session.prepare().await.unwrap().is_empty());
    drop(session);
    fixture.wait_for_teardown().await;

    assert!(
        fixture.head(journal).await > 0,
        "the delta reached the journal"
    );

    // A well-formed acknowledgement of records this journal does not hold, which
    // is what a client's acknowledgement looks like once fragments are destroyed.
    let mut donor = daemon.session().await;
    let mount = donor
        .open(fixture.provisioned("acmeCo/disk/destroyed-donor").await)
        .await
        .unwrap();
    copy_through(&fixture.source("donor"), &mount);

    let foreign_ack = donor.prepare().await.unwrap();
    () = donor.commit(foreign_ack.clone()).await.unwrap();
    () = donor.close().await;

    let mut session = daemon.session().await;
    let open = proto::Open {
        recovered_acks: vec![foreign_ack],
        ..fixture.provisioned(journal).await
    };
    let status = session.open(open).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert!(status.message().contains("applied nothing"), "{status}");
    () = session.ended().await;
}

/// Every protocol violation ends its session, and the disk it held goes with it.
async fn protocol_violations_are_terminal(fixture: &Fixture, daemon: &Daemon) {
    // Preparing twice, when the first delta is still owed a commit.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned("acmeCo/disk/twice-published").await)
        .await
        .unwrap();

    copy_through(&fixture.source("once"), &mount);
    assert!(!session.prepare().await.unwrap().is_empty());

    let status = session.prepare().await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(status.message().contains("awaiting its commit"), "{status}");
    () = session.ended().await;

    // Committing with nothing prepared.
    let mut session = daemon.session().await;
    _ = session
        .open(fixture.provisioned("acmeCo/disk/early-commit").await)
        .await
        .unwrap();

    let status = session
        .commit(bytes::Bytes::from_static(b"not an acknowledgement"))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(status.message().contains("no prepared delta"), "{status}");
    () = session.ended().await;

    // Committing bytes which are not the ones prepared.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned("acmeCo/disk/wrong-commit").await)
        .await
        .unwrap();

    copy_through(&fixture.source("once"), &mount);
    let mut ack = session.prepare().await.unwrap().to_vec();
    *ack.last_mut().unwrap() ^= 0xff;

    let status = session.commit(ack.into()).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(
        status.message().contains("differs from the prepared one"),
        "{status}",
    );
    () = session.ended().await;

    // Opening a second disk on one session.
    let mut session = daemon.session().await;
    _ = session
        .open(fixture.provisioned("acmeCo/disk/twice-opened").await)
        .await
        .unwrap();

    let status = session
        .open(fixture.provisioned("acmeCo/disk/twice-opened").await)
        .await
        .unwrap_err();

    assert!(status.message().contains("exactly one disk"), "{status}");
    () = session.ended().await;

    // A request before Open. Every session must begin with Open.
    let mut session = daemon.session().await;
    let status = session.prepare().await.unwrap_err();

    assert!(status.message().contains("must be Open"), "{status}");
    () = session.ended().await;

    fixture.assert_no_leaks();
}

/// A disk which committed reopens holding the files it committed, over several
/// sequential transactions. Two recoveries of one journal also agree.
async fn a_committed_disk_reopens_with_its_contents(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/reopened";
    let source = fixture.dir.path().join("reopened");

    for generation in 1..=3u8 {
        let mut session = daemon.session().await;
        let mount = session
            .open(fixture.provisioned(journal).await)
            .await
            .unwrap();

        // Every generation but the first opens a disk rebuilt from the journal.
        // That disk holds what the generation before it committed.
        if generation != 1 {
            assert_tree_matches(&source, &format!("{mount}/data"));
        }
        write_source(&source, generation);
        copy_through(&source, &mount);

        let ack = session.prepare().await.unwrap();
        assert!(!ack.is_empty(), "generation {generation} changed the disk");

        () = session.commit(ack).await.unwrap();
        () = session.close().await;
    }

    // Recovery is deterministic. Two more recoveries which commit nothing therefore
    // reproduce the same filesystem both times.
    for _ in 0..2 {
        let mut session = daemon.session().await;
        let mount = session
            .open(fixture.provisioned(journal).await)
            .await
            .unwrap();

        assert_tree_matches(&source, &format!("{mount}/data"));
        () = session.close().await;
    }
}

/// A client made an acknowledgement durable, then failed before it could commit. It
/// hands that acknowledgement back, which repairs the delta.
async fn an_acknowledgement_lost_after_commit_is_repaired(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/repaired";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.source("repaired");

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();

    drop(session);
    fixture.wait_for_teardown().await;

    let mut session = daemon.session().await;
    let mount = session
        .open(proto::Open {
            recovered_acks: vec![ack],
            ..fixture.provisioned(journal).await
        })
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;
}

/// A delta which was prepared but never committed is not disk state. It is
/// discarded, and the disk recovers to the transaction before it.
async fn an_uncommitted_delta_is_discarded(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/uncommitted";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.dir.path().join("uncommitted");

    write_source(&source, 1);
    copy_through(&source, &mount);

    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();

    // A second generation, prepared and never committed.
    let discarded = fixture.dir.path().join("uncommitted-discarded");
    write_source(&discarded, 2);
    copy_through(&discarded, &mount);

    assert!(!session.prepare().await.unwrap().is_empty());
    drop(session);
    fixture.wait_for_teardown().await;

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;
}

/// A journal which holds only the records of a failed first use holds no committed
/// state. Its disk is therefore formatted afresh.
async fn an_orphaned_first_use_yields_a_fresh_disk(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/orphaned";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    copy_through(&fixture.source("orphaned"), &mount);

    assert!(!session.prepare().await.unwrap().is_empty());
    drop(session);
    fixture.wait_for_teardown().await;

    assert!(
        fixture.head(journal).await > 0,
        "the delta reached the journal"
    );

    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    assert_eq!(
        sudo(&["ls", "-A", &mount]).trim(),
        "lost+found",
        "the disk was not formatted afresh",
    );
    () = session.close().await;
}

/// A floor hint seeks a replay and does nothing more. A hint which is absent, or
/// behind, costs replay work and rebuilds the same disk.
async fn a_floor_hint_only_seeks_a_replay(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/floored";
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();
    let source = fixture.source("floored");

    copy_through(&source, &mount);
    let ack = session.prepare().await.unwrap();

    () = session.commit(ack).await.unwrap();
    assert_eq!(session.floor(), 0, "no horizon of this disk completed");
    () = session.close().await;

    // No hint at all, and a clock long before the disk was written. Every fragment
    // is at or after both, so each rebuilds exactly what was committed.
    for floor_hint in [0, 1] {
        let mut session = daemon.session().await;
        let mount = session
            .open(proto::Open {
                floor_hint,
                ..fixture.provisioned(journal).await
            })
            .await
            .unwrap();

        assert_tree_matches(&source, &format!("{mount}/data"));
        assert_eq!(session.floor(), 0, "no horizon was in the replayed range");
        () = session.close().await;
    }
}

/// A boundary cut taken while the filesystem is writing back rebuilds into a
/// filesystem which mounts and passes a consistency check. ext4 replays its own
/// journal over whatever the cut caught in flight.
async fn a_cut_during_writeback_recovers_a_consistent_filesystem(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    let journal = "acmeCo/disk/writeback";
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

    // There is no `fsync`, so the cut lands amongst ext4's own writeback and
    // journal traffic rather than after it.
    let mut writer = std::process::Command::new("sudo")
        .args(["-n", "dd", "if=/dev/urandom", "bs=1M", "count=48"])
        .arg(format!("of={mount}/churn"))
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let ack = session.prepare().await.unwrap();
    () = session.commit(ack).await.unwrap();

    _ = writer.wait().expect("waiting for dd");
    drop(session);
    fixture.wait_for_teardown().await;

    // The daemon mounts what it rebuilds. Opening at all therefore means ext4
    // replayed its journal over the rebuilt image.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.provisioned(journal).await)
        .await
        .unwrap();

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
        .open(fixture.provisioned("acmeCo/disk/disconnected").await)
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

    // Long enough that the write is in flight through the device, rather than
    // still starting up.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(session);

    _ = writer.wait().expect("waiting for dd");
    fixture.wait_for_teardown().await;
}

/// A daemon which is killed outright leaves nothing behind once a daemon takes
/// its directory again.
///
/// The kernel removes the block device when the process serving it dies, so nothing
/// can reach the disk. A killed daemon cannot unmount the filesystem over that
/// device, and it cannot delete the character device under it. The next daemon
/// reclaims both.
async fn a_killed_daemon_leaves_no_mounts(fixture: &Fixture) {
    let daemon = Daemon::start(fixture, "killed").await;
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.provisioned("acmeCo/disk/killed").await)
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

    // Nothing is left for a reaper to find. The daemon deleted the one device it
    // could prove was its own.
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
    /// Root of the broker's `file:///` fragment store. The horizon cases delete
    /// files from it, to prove a recovery reads nothing below its floor.
    fragment_root: std::path::PathBuf,
    /// Interval at which brokers re-list that store. It decides whether a broker can
    /// still serve a deleted fragment locally.
    refresh_interval_seconds: u64,
}

impl Fixture {
    /// Create `journal` as a disk's caller does, and return the `Open` of it.
    /// The daemon creates no journal, so every case which opens a disk begins
    /// here.
    async fn provisioned(&self, journal: &str) -> proto::Open {
        common::create_journal(&self.client, self.spec(journal))
            .await
            .expect("creating a disk journal");

        self.open(journal)
    }

    /// Spec of a disk journal in the test broker's file root.
    ///
    /// SNAPPY is the codec the design specifies for disk journals. Its fragments
    /// live on the broker's own filesystem, and the test has no transport to
    /// fetch them, so the broker decompresses them here.
    /// `gazette::journal::read` covers its own decoder.
    fn spec(&self, journal: &str) -> broker::JournalSpec {
        common::journal_spec(
            journal,
            broker::CompressionCodec::Snappy,
            self.refresh_interval_seconds,
        )
    }

    /// Open of a disk, without creating its journal. A case which wants the
    /// journal absent, or built by hand, uses this.
    fn open(&self, journal: &str) -> proto::Open {
        proto::Open {
            journal: journal.to_string(),
            device_size: DEVICE_SIZE,
            broker: Some(proto::Broker {
                endpoint: self.endpoint.clone(),
                credential: self.credential.clone(),
            }),
            recovered_acks: Vec::new(),
            floor_hint: 0,
        }
    }

    /// A source tree of files. A case copies it through a mount.
    fn source(&self, name: &str) -> std::path::PathBuf {
        let path = self.dir.path().join(name);
        write_source(&path, 1);
        path
    }

    /// Apply every committed delta of `journal` to a fresh image at `path`, and
    /// return the blocks they covered.
    ///
    /// This is a recovering session in miniature. It groups records by the
    /// acknowledgement which commits them, and it drops a delta which was never
    /// acknowledged.
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
                covered += chunk::covered_blocks(&chunk).len();
                chunk::apply(&chunk, &image, &mut allocated).expect("applying a chunk");
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

        // A loop mount lets an unprivileged test read a filesystem which only the
        // kernel can interpret. It replays the ext4 journal, exactly as a recovered
        // disk's mount does.
        sudo(&["mount", "-o", "loop", path(image), path(&mount)]);

        let diff = tree_diff(source, path(&mount.join("data")));
        let _ = sudo(&["umount", path(&mount)]);

        assert!(diff.is_none(), "{}", diff.unwrap());
    }

    /// Journal range a replay would now read. It runs from the earliest fragment
    /// still in the store through to the write head.
    async fn retained_range(&self, journal: &str) -> i64 {
        let begin = std::fs::read_dir(self.fragment_root.join(journal))
            .expect("a fragment store")
            .filter_map(|entry| {
                // A fragment is named for the offsets it covers.
                let name = entry.ok()?.file_name().to_string_lossy().into_owned();
                i64::from_str_radix(name.split('-').next()?, 16).ok()
            })
            .min()
            .expect("a fragment of the journal survived");

        self.head(journal).await - begin
    }

    /// Delete every persisted fragment of `journal` written before `seconds`. That is
    /// exactly the content a replay which seeks from that floor skips.
    fn delete_fragments_before(&self, journal: &str, seconds: u64) -> usize {
        let mut deleted = 0;

        for entry in std::fs::read_dir(self.fragment_root.join(journal)).expect("a fragment store")
        {
            let entry = entry.expect("a fragment");

            let modified = entry
                .metadata()
                .expect("fragment metadata")
                .modified()
                .expect("a fragment modification time")
                .duration_since(std::time::UNIX_EPOCH)
                .expect("a modification time after the epoch")
                .as_secs();

            if modified < seconds {
                std::fs::remove_file(entry.path()).expect("removing a fragment");
                deleted += 1;
            }
        }
        deleted
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

    /// Broker-confirmed write head. It bounds a read, and a transaction which
    /// appended nothing leaves it unchanged.
    async fn head(&self, journal: &str) -> i64 {
        disk_daemon::journal::fence::probe(&self.client, journal)
            .await
            .expect("probing a journal")
            .head
    }

    /// Ask the broker to suspend `journal`, exactly as its own idle pulse
    /// eventually would. `IfFlushed` fully suspends an empty journal, and `Now`
    /// suspends one over content without waiting for a flush.
    ///
    /// The broker answers SUSPENDED after re-resolving the journal it just
    /// suspended, which is how a suspension reports that it took effect.
    async fn suspend(&self, journal: &str, mode: broker::append_request::Suspend) {
        let request = broker::AppendRequest {
            journal: journal.to_string(),
            suspend: mode as i32,
            ..Default::default()
        };
        let stream = self.client.append(request, || {
            futures::stream::empty::<std::io::Result<bytes::Bytes>>()
        });
        futures::pin_mut!(stream);

        loop {
            match futures::StreamExt::next(&mut stream).await {
                Some(Ok(_response)) => return,
                Some(Err(gazette::RetryError {
                    inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                    ..
                })) => return,
                // A just-created journal answers NoJournalPrimaryBroker while
                // its replica is still being assigned.
                Some(Err(gazette::RetryError { inner, .. })) if inner.is_transient() => (),
                other => panic!("suspending {journal}: {other:?}"),
            }
        }
    }

    /// Suspension recorded on `journal`'s spec. Absent while Gazette has never
    /// suspended it; a resumed journal keeps the record at level NONE.
    async fn suspension(&self, journal: &str) -> Option<broker::journal_spec::Suspend> {
        let response = self
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

        response
            .journals
            .into_iter()
            .filter_map(|listed| listed.spec)
            .find(|spec| spec.name == journal)
            .and_then(|spec| spec.suspend)
    }

    /// Value of `journal`'s `author` register, which only a fence installs.
    async fn author(&self, journal: &str) -> Option<String> {
        disk_daemon::journal::fence::probe(&self.client, journal)
            .await
            .expect("probing a journal")
            .author
    }

    fn assert_no_leaks(&self) {
        common::assert_no_leaked_devices();
        common::assert_no_mounts_under(path(self.dir.path()));
    }

    /// Wait for a teardown which the session cannot report. Either the client or
    /// the daemon is gone.
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
        Self::start_with(fixture, name, &[]).await
    }

    async fn start_with(fixture: &Fixture, name: &str, extra: &[&str]) -> Self {
        let dir = fixture.dir.path();
        let uds_path = dir.join(format!("{name}.sock"));

        for sub in ["images", "mounts"] {
            std::fs::create_dir_all(dir.join(format!("{name}-{sub}"))).unwrap();
        }
        let mut command = async_process::Command::new("sudo");
        command
            .args(["-n", env!("CARGO_BIN_EXE_flow-disk-daemon"), "serve"])
            .arg("--uds-path")
            .arg(&uds_path)
            .arg("--image-dir")
            .arg(dir.join(format!("{name}-images")))
            .arg("--mount-dir")
            .arg(dir.join(format!("{name}-mounts")))
            .args(extra);

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
            floor: 0,
        }
    }

    /// End the daemon as systemd does. Wait for it to tear down every disk it
    /// served.
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

    /// What the daemon's process costs the host right now.
    fn cost(&self) -> Cost {
        let pid = self.pid();

        let names: Vec<String> = std::fs::read_dir(format!("/proc/{pid}/task"))
            .expect("the daemon's threads")
            .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path().join("comm")).ok())
            .map(|comm| comm.trim_end().to_string())
            .collect();

        Cost {
            threads: names.len(),
            owners: names
                .iter()
                .filter(|name| name.starts_with("disk-"))
                .count(),
            workers: names
                .iter()
                .filter(|name| name.starts_with("iou-wrk"))
                .count(),
            // Only the daemon's own user may list them.
            files: sudo(&["ls", &format!("/proc/{pid}/fd")]).lines().count(),
        }
    }

    /// Process id of the daemon. The daemon is the child of the `sudo` this test
    /// spawned, and not that `sudo` itself.
    fn pid(&self) -> u32 {
        let matched = std::process::Command::new("pgrep")
            .args(["-f", &self.pattern()])
            .output()
            .expect("spawning pgrep");

        String::from_utf8_lossy(&matched.stdout)
            .trim()
            .parse()
            .expect("exactly one daemon serves this socket")
    }

    /// Signal the daemon. It runs as root, so only `sudo` can reach it.
    fn signal(&self, signal: &str) {
        _ = std::process::Command::new("sudo")
            .args(["-n", "pkill", &format!("-{signal}"), "-f"])
            .arg(self.pattern())
            .status()
            .expect("spawning pkill");
    }

    /// Whether this daemon's process is still around. Signal zero asks that without
    /// delivering anything.
    fn running(&self) -> bool {
        std::process::Command::new("sudo")
            .args(["-n", "pkill", "-0", "-f"])
            .arg(self.pattern())
            .status()
            .expect("spawning pkill")
            .success()
    }

    /// Command line of this daemon. It is anchored, so it matches the daemon and not
    /// the `sudo` which spawned it. That `sudo` line holds the same path.
    fn pattern(&self) -> String {
        format!(
            "^{} serve --uds-path {}",
            env!("CARGO_BIN_EXE_flow-disk-daemon"),
            self.uds_path.display(),
        )
    }
}

/// What a daemon's process costs the host. A unit file's `TasksMax` and
/// `LimitNOFILE` have to cover this.
#[derive(Debug)]
struct Cost {
    threads: usize,
    /// Of those, the one thread each disk is served by.
    owners: usize,
    /// Of those, the kernel's own `io_uring` workers.
    workers: usize,
    /// Descriptors held, of which a disk's own are its image, its character
    /// device, its ring, and its wake.
    files: usize,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Without this, a test which failed part way would leave a privileged
        // process behind, and the next test's leak checks would see its devices.
        //
        // `KILL` is the last resort rather than the first. A daemon killed outright
        // while a device request is in flight leaves a device the host cannot
        // remove. The kernel cannot complete that request, so the process never
        // exits, and every later `ublk` control command blocks behind it. Only a
        // reboot clears that.
        self.signal("TERM");
        let deadline = std::time::Instant::now() + TEARDOWN;

        while std::time::Instant::now() < deadline {
            if !self.running() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        self.signal("KILL");
    }
}

/// One session's stream, which serves exactly one disk.
struct Session {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    responses: tonic::Streaming<proto::Response>,
    /// Greatest recovery floor the daemon has reported, which is what a client
    /// persists and hands back as the next `Open`'s hint.
    floor: u64,
}

impl Session {
    /// Create the disk and return its mount path.
    async fn open(&mut self, open: proto::Open) -> tonic::Result<String> {
        match self.request(proto::request::Request::Open(open)).await? {
            proto::response::Response::Opened(opened) => {
                self.floor = std::cmp::max(self.floor, opened.floor);
                Ok(opened.mount_path)
            }
            response => panic!("expected Opened, got {response:?}"),
        }
    }

    /// Recovery floor this session has been told, and zero while it has been told
    /// none.
    fn floor(&self) -> u64 {
        self.floor
    }

    /// Cut a delta and return its acknowledgement. It is empty when the disk did
    /// not change.
    async fn prepare(&mut self) -> tonic::Result<bytes::Bytes> {
        match self
            .request(proto::request::Request::Prepare(proto::Prepare {}))
            .await?
        {
            proto::response::Response::Prepared(prepared) => Ok(prepared.ack),
            response => panic!("expected Prepared, got {response:?}"),
        }
    }

    async fn commit(&mut self, ack: bytes::Bytes) -> tonic::Result<()> {
        match self
            .request(proto::request::Request::Commit(proto::Commit { ack }))
            .await?
        {
            proto::response::Response::Committed(proto::Committed { floor }) => {
                self.floor = std::cmp::max(self.floor, floor);
                Ok(())
            }
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

    /// End the session as a client does. Wait for the daemon to finish tearing its
    /// disk down.
    async fn close(mut self) {
        drop(self.requests);
        assert_eq!(self.responses.message().await.expect("a clean close"), None,);
    }

    /// Wait for a failed session to end. The daemon ends it only once the disk is
    /// destroyed.
    async fn ended(mut self) {
        drop(self.requests);
        assert!(matches!(self.responses.message().await, Ok(None) | Err(_)));
    }
}

/// List `journal`, which every label read and write here begins with.
async fn list(client: &gazette::journal::Client, journal: &str) -> broker::ListResponse {
    client
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
        .expect("listing a journal")
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

/// How `dir` differs from the `source` tree, or `None` when it does not. The mounts
/// are root-owned, so a comparison of them is privileged too.
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

/// Require `image` to hold a filesystem which is consistent once its own journal is
/// replayed, and which needs no repair beyond that.
fn assert_fsck_clean(image: &std::path::Path) {
    // The first pass replays the filesystem journal. That modifies the image, and
    // e2fsck reports the modification. The second pass must find nothing at all.
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

/// Run a privileged command. This is how a test reaches a root-owned mount.
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

/// Write a source tree whose content exercises the chunk codec. It holds a file
/// smaller than a block, a file of whole blocks, an entirely zero file, and one file
/// large enough to span records.
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

/// Content in which every third block is entirely zero. Both trailing-zero trimming
/// and empty-data chunks then occur.
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
    credential_lasting(cluster, 3600)
}

/// The same token, expiring in `seconds`. A case can then outlive the credential it
/// opened with.
fn credential_lasting(cluster: &e2e_support::GazetteCluster, seconds: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = proto_gazette::Claims {
        cap: proto_gazette::capability::LIST
            | proto_gazette::capability::APPLY
            | proto_gazette::capability::READ
            | proto_gazette::capability::APPEND,
        exp: now + seconds,
        iat: now,
        iss: "disk-daemon-test".to_string(),
        sel: broker::LabelSelector::default(),
        sub: "disk-daemon-test".to_string(),
    };

    tokens::jwt::sign(claims, &cluster.encode_key).unwrap()
}
