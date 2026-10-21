#![allow(dead_code, unused_imports)]

pub mod attachment;
pub mod tree;

pub use attachment::Attachment;
pub use tree::{Tree, pattern};

use proto_gazette::broker;

/// 128 MiB. `mkfs.ext4` accepts that size comfortably, and it keeps a case to a few
/// seconds.
pub const DEVICE_SIZE: u64 = 128 * 1024 * 1024;

/// How long a case waits for a teardown it cannot observe over an attachment, because
/// either the client or the daemon is already gone.
pub const TEARDOWN: std::time::Duration = std::time::Duration::from_secs(30);

/// Refresh interval of the brokers of an ordinary case: long enough that nothing
/// re-lists the fragment store under it.
const SLOW_REFRESH_SECONDS: u64 = 5 * 60;

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
    /// Owns the brokers and etcd shared by this test binary.
    data_plane: e2e_support::DataPlane,
    /// Broker endpoint passed to daemon processes.
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

    /// Default disk-journal spec. Fragments use Snappy in the brokers' `file:///`
    /// root, so reads go through broker decompression. Tests can stage another spec
    /// before opening; [`Fixture::open`] preserves existing journals.
    pub fn spec(&self, journal: &str) -> broker::JournalSpec {
        broker::JournalSpec {
            name: journal.to_string(),
            replication: 1,
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

    /// Create the journal if absent and build its `Open` request. Existing specs
    /// survive, allowing tests to stage custom configuration first.
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

    /// Stage a custom journal spec before opening, preserving any existing journal.
    pub async fn create_journal(&self, spec: broker::JournalSpec) {
        let request = broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: 0,
                upsert: Some(spec),
                delete: String::new(),
            }],
        };

        match self.client.apply(request).await {
            Ok(_) | Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => (),
            Err(err) => panic!("staging a journal: {err}"),
        }
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
        let response = self
            .append_nothing(journal, broker::append_request::Suspend::Resume)
            .await
            .expect("a resuming append is never refused as suspended");

        response.commit.map_or(0, |commit| commit.end)
    }

    /// Request suspension as the broker's idle pulse does. `IfFlushed` fully
    /// suspends an empty journal; `Now` suspends content without waiting for flush.
    /// A SUSPENDED reply confirms the requested suspension.
    pub async fn suspend(&self, journal: &str, mode: broker::append_request::Suspend) {
        _ = self.append_nothing(journal, mode).await;
    }

    /// The broker reports a journal's head only in an append response, so read it
    /// with an append of nothing. `None` if the broker answers that it is suspended.
    async fn append_nothing(
        &self,
        journal: &str,
        suspend: broker::append_request::Suspend,
    ) -> Option<broker::AppendResponse> {
        let request = broker::AppendRequest {
            journal: journal.to_string(),
            suspend: suspend as i32,
            ..Default::default()
        };
        let mut stream = std::pin::pin!(self.client.append(request, futures::stream::empty));

        loop {
            match futures::StreamExt::next(&mut stream)
                .await
                .expect("an append stream does not end without a response")
            {
                Ok(response) => return Some(response),
                Err(gazette::RetryError {
                    inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                    ..
                }) => return None,
                // Polling again pays the stream's backoff and restarts route discovery.
                Err(gazette::RetryError { inner, .. }) if inner.is_transient() => (),
                Err(gazette::RetryError { inner, .. }) => panic!("appending to {journal}: {inner}"),
            }
        }
    }

    /// Suspension recorded on `journal`'s spec. Absent while Gazette has never
    /// suspended it; a resumed journal keeps the record at level NONE.
    pub async fn suspension(&self, journal: &str) -> Option<broker::journal_spec::Suspend> {
        self.listed(journal).await.and_then(|spec| spec.suspend)
    }

    /// Read the daemon's recovery floor from the journal label, as a pruner does.
    pub async fn stored_floor(&self, journal: &str) -> Option<u64> {
        let Some(spec) = self.listed(journal).await else {
            return None; // A journal which does not exist stores no floor.
        };
        let set = spec.labels.unwrap_or_default();

        match labels::maybe_one(&set, labels::DISK_RECOVERY_FLOOR).expect("one floor label") {
            "" => None,
            value => Some(u64::from_str_radix(value, 16).expect("a floor label")),
        }
    }

    /// Set or remove a floor for recovery tests. Unlike daemon updates, this may
    /// move backward to simulate a stale label or convergence dropping it.
    pub async fn plant_floor(&self, journal: &str, floor: Option<u64>) {
        let listed = self
            .client
            .get_journal(journal)
            .await
            .expect("listing a journal")
            .expect("the journal exists");
        let mut spec = listed.spec.expect("a listed journal has a spec");
        let set = spec.labels.take().unwrap_or_default();

        spec.labels = Some(match floor {
            Some(floor) => {
                labels::set_value(set, labels::DISK_RECOVERY_FLOOR, &format!("{floor:016x}"))
            }
            None => labels::remove(set, labels::DISK_RECOVERY_FLOOR),
        });

        // Compare-and-swap against the listed revision, so a concurrent change fails
        // the case rather than being overwritten.
        _ = self
            .client
            .apply(broker::ApplyRequest {
                changes: vec![broker::apply_request::Change {
                    expect_mod_revision: listed.mod_revision,
                    upsert: Some(spec),
                    delete: String::new(),
                }],
            })
            .await
            .expect("planting a recovery floor");
    }

    /// Where the brokers persist fragments, for the case which prunes them.
    pub fn fragment_root(&self) -> &std::path::Path {
        self.data_plane.gazette.fragment_root.as_path()
    }

    /// Signal every broker. SIGSTOP/SIGCONT simulate an outage with no replies,
    /// which clients must wait through.
    pub fn signal_brokers(&self, signal: rustix::process::Signal) {
        for broker in &self.data_plane.gazette.brokers {
            let pid = broker.process.id();

            () = rustix::process::kill_process(
                rustix::process::Pid::from_raw(pid as i32).expect("a broker's pid is positive"),
                signal,
            )
            .unwrap_or_else(|err| panic!("signalling {pid}: {err}"));
        }
    }

    pub fn assert_no_leaks(&self) {
        () = assert_no_leaked_devices();
        () = assert_no_mounts_under(self.path());
    }

    /// Wait for a teardown which no attachment can report, because either the client or
    /// the daemon is gone, and then require that it left nothing behind.
    pub async fn wait_for_teardown(&self) {
        let deadline = std::time::Instant::now() + TEARDOWN;

        loop {
            let blocks = entries("/sys/block", |name| name.starts_with("ublkb"));
            if blocks.is_empty() {
                break;
            }
            // The daemon logs each step of a teardown, so its output shows which step
            // stalled. A device no log mentions belongs to another daemon on this host.
            assert!(
                std::time::Instant::now() < deadline,
                "block devices {blocks:?} outlived a teardown of {TEARDOWN:?}",
            );
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

/// Run the shipped daemon through `sudo -n`, leaving Cargo and filesystem
/// workloads unprivileged. Returned mounts belong to the client.
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

    /// One raw attachment stream, which serves exactly one disk.
    ///
    /// A case uses this where it must send what [`disk_daemon::client::Disk`] cannot
    /// express: a request out of turn, an acknowledgement of bytes nothing prepared,
    /// or two requests before either reply is read.
    pub async fn attach(&self) -> attachment::Attachment {
        attachment::Attachment::open(self.connect().await.expect("connect")).await
    }

    /// Each thread of the daemon, by name, with the flags word the kernel keeps for
    /// it. `/proc` shows a root process's threads and their flags to anyone.
    pub fn threads(&self) -> Vec<(String, u64)> {
        let output = std::process::Command::new("pgrep")
            .arg("-f")
            .arg(self.pattern())
            .output()
            .expect("spawning pgrep");
        let pid = String::from_utf8_lossy(&output.stdout);
        let pid = pid.trim();
        assert!(!pid.is_empty(), "no process matches {:?}", self.pattern());

        std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .map(|task| {
                let task = task.unwrap().path();
                let name = std::fs::read_to_string(task.join("comm")).unwrap();
                let stat = std::fs::read_to_string(task.join("stat")).unwrap();

                // The flags word is the seventh field after the parenthesized name.
                let flags = stat
                    .rsplit_once(')')
                    .unwrap()
                    .1
                    .split_whitespace()
                    .nth(6)
                    .unwrap()
                    .parse()
                    .unwrap();

                (name.trim().to_string(), flags)
            })
            .collect()
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
        // Clean up even when a test fails. Try TERM before KILL: killing with
        // ublk requests in flight can leave a device that blocks later control
        // commands and requires a host reboot.
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

/// Require open to fail, closing any unexpectedly returned disk first.
/// `Disk` is not `Debug`, so `expect_err` cannot be used directly.
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

/// Prepare, acknowledge, and await confirmation before inspecting journal state.
/// Return the acknowledgement for tests that supply it at replacement promotion.
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

/// Whether anything is mounted at `path`.
pub fn is_mounted(path: &std::path::Path) -> bool {
    let path = path.to_str().expect("paths of a tempdir are UTF-8");

    std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .any(|line| line.contains(path))
}

/// Wait for the disk of an attachment which is gone to be torn down. Its client cannot
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

/// Wait until writes are parked at the device mounted at `mount`: some are in flight
/// there, and none completes. A disk parks its writes once its recording channel is
/// full, as it is while its brokers answer nothing.
pub async fn wait_for_parked_writes(mount: &std::path::Path) {
    let mount = mount.to_str().expect("paths of a tempdir are UTF-8");
    let device = std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            let device = fields.next()?;
            (fields.next()? == mount).then(|| device.trim_start_matches("/dev/").to_string())
        })
        .unwrap_or_else(|| panic!("nothing is mounted at {mount}"));

    // Writes completed, and requests in flight, per the kernel's
    // `Documentation/block/stat.rst`.
    let stat = || {
        let stat = std::fs::read_to_string(format!("/sys/block/{device}/stat")).unwrap();
        let fields: Vec<u64> = stat
            .split_whitespace()
            .map(|field| field.parse().unwrap())
            .collect();
        (fields[4], fields[8])
    };
    let deadline = std::time::Instant::now() + TEARDOWN;
    let mut last = stat();

    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let now = stat();

        if now.1 != 0 && last.1 != 0 && now.0 == last.0 {
            return;
        }
        last = now;
    }
    panic!("no write parked at {device}");
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
