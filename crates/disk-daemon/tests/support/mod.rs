//! Harness of the black-box suite.
//!
//! Every case in `tests/` drives `flow-disk-daemon` exactly as it ships. It reaches
//! the daemon over `disk_daemon::client` or, where a case must send what that client cannot
//! express, over the tenure gRPC. It reads and writes its disks through the
//! mounts the daemon returns, with ordinary file I/O. It observes what the daemon left
//! in Gazette with a journal client of its own. The generic half of that — listing a
//! journal, probing its head, and creating one — is `e2e_support`, so what is here is
//! what is about disks.
//!
//! Nothing here reaches into the crate's implementation modules. A case therefore says
//! what the daemon owes its client and its brokers, and not how the daemon is built,
//! so these cases are what a refactoring of the daemon is answerable to. Tests of the
//! parts themselves live beside the code, in `src/`.
//!
//! Each binary of the suite includes this module and uses part of it, so unused items
//! and unused re-exports here are the rule rather than a smell.
#![allow(dead_code, unused_imports)]

pub mod tenure;
pub mod tree;

pub use tenure::Tenure;
pub use tree::{Tree, pattern};

use proto_gazette::broker;

/// 128 MiB. `mkfs.ext4` accepts that size comfortably, and it keeps a case to a few
/// seconds.
pub const DEVICE_SIZE: u64 = 128 * 1024 * 1024;

/// How long a case waits for a teardown it cannot observe over a tenure, because
/// either the client or the daemon is already gone.
pub const TEARDOWN: std::time::Duration = std::time::Duration::from_secs(30);

/// Refresh interval of the brokers of an ordinary case: long enough that nothing
/// re-lists the fragment store under it.
const SLOW_REFRESH_SECONDS: u64 = 5 * 60;

/// Whether anything is mounted at `path`.
pub fn is_mounted(path: &std::path::Path) -> bool {
    let path = path.to_str().expect("paths of a tempdir are UTF-8");

    std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .any(|line| line.contains(path))
}

/// Fail with what to do about it, rather than skip. A machine which cannot serve a
/// device then says so.
fn check_prerequisites() {
    assert!(
        std::path::Path::new("/sys/module/ublk_drv").exists(),
        "ublk_drv is not loaded, so no block device can be served. \
         Load it with `sudo modprobe ublk_drv`.",
    );
    assert!(
        std::path::Path::new("/dev/ublk-control").exists(),
        "/dev/ublk-control is absent though ublk_drv is loaded, so this kernel's \
         module was built without the control device.",
    );

    let sudo = std::process::Command::new("sudo")
        .args(["-n", "true"])
        .output()
        .expect("spawning sudo");

    assert!(
        sudo.status.success(),
        "passwordless sudo is required, because these tests serve real ublk devices \
         from `sudo -n` child processes: {}",
        String::from_utf8_lossy(&sudo.stderr),
    );
}

/// No device node and no `/sys/block` entry outlives the case which made it.
///
/// This waits rather than asserting outright. `devtmpfs` unlinks a node slightly
/// after the command which removed its device returns, exactly as it creates one
/// slightly after the command which added it.
fn assert_no_leaked_devices() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        let nodes = entries("/dev", |name| {
            name.starts_with("ublk") && name != "ublk-control"
        });
        let blocks = entries("/sys/block", |name| name.starts_with("ublkb"));

        if nodes.is_empty() && blocks.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "leaked device nodes {nodes:?} and block devices {blocks:?}",
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// No mount of `dir` or of a served device outlives the case which made it.
fn assert_no_mounts_under(dir: &str) {
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap();

    for line in mounts.lines() {
        assert!(
            !line.contains(dir) && !line.contains("/dev/ublkb"),
            "leaked mount: {line}",
        );
    }
}

fn entries(dir: &str, keep: impl Fn(&str) -> bool) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
        .filter(|name| keep(name))
        .collect()
}

/// The data plane a binary's cases share, and where their disks live.
///
/// A data plane is expensive to start, so one binary starts one and works a journal
/// per case. [`Fixture::stop`] ends it.
pub struct Fixture {
    /// Images, mounts, and sockets of the daemons a case starts.
    pub dir: tempfile::TempDir,
    /// Client of the case itself, which lists journals, reads their labels and
    /// registers, and probes their heads.
    pub client: gazette::journal::Client,
    data_plane: e2e_support::DataPlane,
    endpoint: String,
    /// Base64 HMAC keys of the data plane, which the daemon signs its own broker
    /// tokens with.
    auth_keys: String,
    /// Interval at which brokers re-list their fragment store. It decides whether a
    /// broker can still serve a deleted fragment from a local spool file.
    refresh_interval_seconds: u64,
}

impl Fixture {
    /// Start a data plane whose brokers hold their fragment listings.
    pub async fn start() -> Self {
        Self::start_refreshing(SLOW_REFRESH_SECONDS).await
    }

    /// [`Fixture::start`], with brokers which re-list the fragment store every
    /// `refresh_interval_seconds`. A case which deletes a fragment needs a short
    /// interval, so that what it deleted is content no broker still holds open.
    pub async fn start_refreshing(refresh_interval_seconds: u64) -> Self {
        () = check_prerequisites();

        let data_plane =
            e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
                .await
                .expect("DataPlane start");

        Self {
            dir: tempfile::tempdir().expect("tempdir"),
            client: data_plane.journal_client.clone(),
            endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
            auth_keys: data_plane.gazette.auth_keys.clone(),
            refresh_interval_seconds,
            data_plane,
        }
    }

    /// End the data plane, having required that the binary left nothing behind.
    pub async fn stop(self) {
        () = self.assert_no_leaks();

        self.data_plane
            .graceful_stop()
            .await
            .expect("DataPlane graceful_stop");
    }

    /// Spec of a disk journal stored in the test brokers' `file:///` root.
    ///
    /// The daemon creates no journal, so [`Fixture::open`] creates one from this spec
    /// exactly as a Flow activation would. A case which needs a different spec stages
    /// it with [`Fixture::create_journal`] before it opens.
    ///
    /// SNAPPY is the codec the design specifies for disk journals. Its fragments live
    /// on a broker's own filesystem and a case has no transport to fetch them, so the
    /// broker decompresses whatever a case reads back.
    pub fn spec(&self, journal: &str) -> broker::JournalSpec {
        broker::JournalSpec {
            name: journal.to_string(),
            replication: 1,
            // A disk's journal declares that it holds one, as a recovery log declares
            // that it holds a recovery log.
            labels: Some(labels::build_set([(
                labels::CONTENT_TYPE,
                disk_daemon::CONTENT_TYPE_DISK,
            )])),
            fragment: Some(broker::journal_spec::Fragment {
                length: 1 << 20,
                compression_codec: broker::CompressionCodec::Snappy as i32,
                stores: vec!["file:///".to_string()],
                refresh_interval: Some(
                    std::time::Duration::from_secs(self.refresh_interval_seconds).into(),
                ),
                // Long enough that only fragment length closes one, so a case decides
                // where its fragment boundaries fall.
                flush_interval: Some(std::time::Duration::from_secs(48 * 3600).into()),
                retention: None,
                path_postfix_template: String::new(),
            }),
            flags: broker::journal_spec::Flag::ORdwr as u32,
            max_append_rate: 1 << 22,
            suspend: None,
        }
    }

    /// `Open` of a disk, having created its journal from [`Fixture::spec`].
    ///
    /// The daemon creates no journal, so this is the harness standing in for whoever
    /// deploys a disk. A case which staged its own spec still calls this: the create
    /// is insert-only, so the staged spec survives.
    pub async fn open(&self, journal: &str) -> disk_daemon::proto::Open {
        self.open_with(self.spec(journal)).await
    }

    /// [`Fixture::open`] against a spec a case built for itself.
    pub async fn open_with(&self, spec: broker::JournalSpec) -> disk_daemon::proto::Open {
        let journal = spec.name.clone();
        () = self.create_journal(spec).await;

        self.open_absent(&journal)
    }

    /// `Open` of a journal which nothing creates, for a case about what the daemon
    /// does with a name that does not exist.
    pub fn open_absent(&self, journal: &str) -> disk_daemon::proto::Open {
        disk_daemon::proto::Open {
            journal: journal.to_string(),
            device_size: DEVICE_SIZE,
        }
    }

    /// Create `journal` unless it exists.
    ///
    /// [`Fixture::open`] does this for the spec every ordinary case uses. A case calls
    /// this directly to stage a journal of its own: one which a disk cannot be
    /// recovered from, or one Gazette has suspended.
    pub async fn create_journal(&self, spec: broker::JournalSpec) {
        _ = e2e_support::journals::create(&self.client, spec)
            .await
            .expect("staging a journal");
    }

    /// Live spec of `journal`, or `None` where no such journal exists.
    pub async fn listed(&self, journal: &str) -> Option<broker::JournalSpec> {
        self.client
            .get_journal(journal)
            .await
            .expect("listing a journal")
            .and_then(|listed| listed.spec)
    }

    /// Whether the journal exists at all.
    pub async fn exists(&self, journal: &str) -> bool {
        self.listed(journal).await.is_some()
    }

    /// Broker-confirmed write head. It bounds a read, and a transaction which appended
    /// nothing leaves it unchanged.
    ///
    /// This resumes a suspended journal as any append does. A case which is about
    /// suspension reads [`Fixture::suspension`] instead.
    pub async fn head(&self, journal: &str) -> i64 {
        e2e_support::journals::head(&self.client, journal)
            .await
            .expect("probing a journal")
    }

    /// Value of `journal`'s `author` register, which only a fence installs.
    ///
    /// The register is the daemon's, and a case reads it to see that an append claimed
    /// the journal it created.
    pub async fn author(&self, journal: &str) -> Option<String> {
        e2e_support::journals::register(&self.client, journal, "author")
            .await
            .expect("probing a journal")
    }

    /// Ask the broker to suspend `journal`, exactly as its own idle pulse eventually
    /// would. `IfFlushed` fully suspends an empty journal, and `Now` suspends one over
    /// content without waiting for a flush.
    ///
    /// The refusal a suspended journal answers with is how the suspension reports that
    /// it took effect, so there is no outcome here for a case to read.
    pub async fn suspend(&self, journal: &str, mode: broker::append_request::Suspend) {
        _ = e2e_support::journals::probe(&self.client, journal, mode)
            .await
            .expect("suspending a journal");
    }

    /// Suspension recorded on `journal`'s spec. Absent while Gazette has never
    /// suspended it; a resumed journal keeps the record at level NONE.
    pub async fn suspension(&self, journal: &str) -> Option<broker::journal_spec::Suspend> {
        self.listed(journal).await.and_then(|spec| spec.suspend)
    }

    /// Recovery floor the daemon stored on `journal`, or `None` for none.
    ///
    /// The daemon keeps this itself, so a case reads the journal rather than a tenure.
    /// It is also where whatever deletes fragments looks.
    pub async fn stored_floor(&self, journal: &str) -> Option<u64> {
        let Some(spec) = self.listed(journal).await else {
            return None; // A journal which does not exist stores no floor.
        };
        let set = spec.labels.unwrap_or_default();

        match labels::maybe_one(&set, disk_daemon::DISK_RECOVERY_FLOOR).expect("one floor label") {
            "" => None,
            value => Some(disk_daemon::parse_recovery_floor(value).expect("a floor label")),
        }
    }

    /// Store `floor` on `journal`, as the daemon itself would.
    ///
    /// This is not monotonic, unlike the daemon's own store: a case uses it to plant
    /// the floor it wants to see a recovery handle.
    pub async fn store_floor(&self, journal: &str, floor: u64) {
        _ = e2e_support::journals::update(&self.client, journal, |spec| {
            spec.labels = Some(labels::set_value(
                spec.labels.take().unwrap_or_default(),
                disk_daemon::DISK_RECOVERY_FLOOR,
                &disk_daemon::recovery_floor_value(floor),
            ));
            true
        })
        .await
        .expect("applying a recovery floor");
    }

    /// Where the brokers persist fragments, for the case which prunes them.
    pub fn fragment_root(&self) -> &std::path::Path {
        self.data_plane.gazette.fragment_root.as_path()
    }

    /// Send `signal` to every broker of the data plane.
    ///
    /// A stopped broker answers nothing while it is stopped, which is the outage a
    /// tenure must park through rather than fail.
    pub fn signal_brokers(&self, signal: libc::c_int) {
        () = self.data_plane.gazette.signal(signal);
    }

    pub fn assert_no_leaks(&self) {
        () = assert_no_leaked_devices();
        () = assert_no_mounts_under(self.path());
    }

    /// Wait for a teardown which no tenure can report, because either the client or
    /// the daemon is gone, and then require that it left nothing behind.
    pub async fn wait_for_teardown(&self) {
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

    pub fn path(&self) -> &str {
        self.dir
            .path()
            .to_str()
            .expect("paths of a tempdir are UTF-8")
    }
}

/// A `flow-disk-daemon` process, run as it ships.
///
/// It runs in a `sudo -n` child, because serving a device and mounting a filesystem
/// need `CAP_SYS_ADMIN` and cargo must not run as root. The mounts it hands back
/// belong to the client which opened them, so a case reads and writes its disks
/// unprivileged.
pub struct Daemon {
    pub uds_path: std::path::PathBuf,
    child: async_process::Child,
}

impl Daemon {
    pub async fn start(fixture: &Fixture, name: &str) -> Self {
        Self::start_with(fixture, name, &[]).await
    }

    /// [`Daemon::start`], with `extra` command-line flags. They are the same flags an
    /// operator sets.
    pub async fn start_with(fixture: &Fixture, name: &str, extra: &[&str]) -> Self {
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
            .args(["--broker-address", &fixture.endpoint])
            .args(["--data-plane-fqdn", "disk-daemon-test"])
            .args(["--data-plane-auth-keys", &fixture.auth_keys])
            .args(extra);

        let child: async_process::Child = command.spawn().expect("spawning the daemon").into();

        let daemon = Self { uds_path, child };
        () = daemon.wait_until_serving().await;

        daemon
    }

    /// The crate's own client of this daemon. Every disk of one client shares its
    /// connection, so a case which serves many disks builds one client.
    pub async fn client(&self) -> disk_daemon::client::Client {
        disk_daemon::client::Client::connect(&self.uds_path)
            .await
            .expect("connecting the client")
    }

    /// One raw tenure stream, which serves exactly one disk.
    ///
    /// A case uses this where it must send what [`disk_daemon::client::Disk`] cannot
    /// express: a request out of turn, an acknowledgement of bytes nothing prepared,
    /// or two requests before either reply is read.
    pub async fn tenure(&self) -> Tenure {
        Tenure::open(self.connect().await.expect("connect")).await
    }

    /// End the daemon as systemd does, and wait for it to tear down every disk it
    /// served.
    pub async fn drain(mut self) {
        () = self.signal("TERM");
        let status = self.child.wait().await.expect("waiting for the daemon");

        assert!(status.success(), "the daemon drained with {status}");
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
            "^{} --uds-path {}",
            env!("CARGO_BIN_EXE_flow-disk-daemon"),
            self.uds_path.display(),
        )
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Without this, a case which failed part way would leave a privileged process
        // behind, and the next case's leak checks would see its devices.
        //
        // `KILL` is the last resort rather than the first. A daemon killed outright
        // while a device request is in flight leaves a device the host cannot remove.
        // The kernel cannot complete that request, so the process never exits, and
        // every later `ublk` control command blocks behind it. Only a reboot clears
        // that.
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

/// The failure of an open which had to be refused.
///
/// [`disk_daemon::client::Disk`] is deliberately not `Debug` — it is a live tenure —
/// so a case cannot say `expect_err` of an open. This ends the disk an open should not
/// have returned, and names the mount it handed back.
pub async fn expect_refused(
    opened: disk_daemon::client::Result<(disk_daemon::client::Disk, std::path::PathBuf)>,
) -> disk_daemon::client::Error {
    match opened {
        Err(err) => err,
        Ok((disk, mount)) => {
            _ = disk.close().await;
            panic!("the open was not refused: it returned {mount:?}");
        }
    }
}

/// [`expect_refused`], of an open the daemon refused as invalid. The status is
/// returned for a case which is also about what it says.
pub async fn expect_invalid(
    opened: disk_daemon::client::Result<(disk_daemon::client::Disk, std::path::PathBuf)>,
) -> tonic::Status {
    let err = expect_refused(opened).await;

    let disk_daemon::client::Error::Invalid(status) = err else {
        panic!("expected an invalid request, got {err:?}");
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");

    status
}

/// Cut a delta of `disk`, commit it, and wait for the daemon to confirm the commit.
///
/// The acknowledgement is returned, because a case which loses a tenure hands it
/// back in the next `Open`. `acknowledge` leaves the commit in flight, so the wait
/// here is what makes the delta durable before the case observes the journal.
pub async fn commit(disk: &mut disk_daemon::client::Disk) -> bytes::Bytes {
    let ack = cut(disk).await;

    () = disk.acknowledge(ack.clone()).await.expect("acknowledge");
    () = disk.acknowledged().await.expect("the commit landed");

    ack
}

/// Cut a delta of `disk` and leave it uncommitted. Its data records are durable and
/// nothing has committed them, which is the prepared phase of a two-phase commit.
pub async fn cut(disk: &mut disk_daemon::client::Disk) -> bytes::Bytes {
    disk.prepare()
        .await
        .expect("prepare")
        .expect("the disk changed")
}

/// Require `disk` to have nothing to cut, which is a transaction that changed nothing.
pub async fn cut_nothing(disk: &mut disk_daemon::client::Disk) {
    assert_eq!(
        disk.prepare().await.expect("prepare"),
        None,
        "the disk changed when nothing wrote to it",
    );
}

/// Wait for the disk of a tenure which is gone to be torn down. Its client cannot
/// observe that teardown, because its stream is already gone.
pub async fn wait_unmounted(mount: &std::path::Path) {
    let deadline = std::time::Instant::now() + TEARDOWN;

    while std::time::Instant::now() < deadline {
        if !is_mounted(mount) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{mount:?} was not torn down");
}

/// Run a command without blocking the runtime, and require that it succeeded.
pub async fn run(program: &str, args: &[&str]) {
    let mut command = async_process::Command::new(program);
    command.args(args);

    let output = async_process::output(&mut command)
        .await
        .unwrap_or_else(|err| panic!("spawning {program} {args:?}: {err}"));

    assert!(
        output.status.success(),
        "{program} {args:?} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Run a privileged command. Only the daemon's own state needs this: a case reads and
/// writes its disks as itself.
pub fn sudo(args: &[&str]) -> String {
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

/// Spawn a writer of `bytes` bytes at `path`, which the case does not wait for.
///
/// A case which must observe a write in flight — parked behind an unreachable broker,
/// or caught by a cut mid-writeback — needs the write to be somebody else's. `sync`
/// decides whether it returns before the filesystem has written it back.
pub fn spawn_writer(path: &std::path::Path, bytes: u64, sync: bool) -> std::process::Child {
    let mut command = std::process::Command::new("dd");
    command
        .args(["if=/dev/urandom", "bs=1M", "status=none"])
        .arg(format!("count={}", bytes / (1 << 20)))
        .arg(format!("of={}", path.display()));

    if sync {
        command.arg("conv=fsync");
    }
    command
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd")
}

/// Wait for a spawned writer to finish, without holding the runtime it must finish on,
/// and require that it wrote what it was asked to.
pub async fn wait_for_writer(writer: &mut std::process::Child) {
    let status = abandon_writer(writer).await;

    assert!(status.success(), "a write failed: {status}");
}

/// Wait for a spawned writer to finish and report how it went.
///
/// A case which takes the device away mid-write uses this: the writer fails, and that
/// failure is the point rather than a problem.
pub async fn abandon_writer(writer: &mut std::process::Child) -> std::process::ExitStatus {
    let deadline = std::time::Instant::now() + TEARDOWN;

    while std::time::Instant::now() < deadline {
        match writer.try_wait().expect("polling a writer") {
            Some(status) => return status,
            None => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    _ = writer.kill();
    panic!("a write neither completed nor failed");
}
