#![allow(dead_code, unused_imports)]

mod attachment;
mod tree;

pub use tree::{BLOCK, Tree, pattern};

use proto_gazette::broker;

/// Large enough for `mkfs.ext4`, and small enough to keep a case to seconds.
pub const DEVICE_SIZE: u64 = 128 * 1024 * 1024;

/// How long a case waits on a teardown or a stalled device before failing.
pub const TEARDOWN: std::time::Duration = std::time::Duration::from_secs(30);

/// Fragment-store refresh interval of most cases: long enough to never re-list.
const SLOW_REFRESH_SECONDS: u64 = 5 * 60;

/// A data plane shared by a test binary's cases, each using its own journals.
/// Starting one is expensive. [`Fixture::stop`] ends it.
pub struct Fixture {
    /// Images, mounts, and sockets of the daemons a case starts.
    pub dir: tempfile::TempDir,
    /// The cases' own journal client.
    pub client: gazette::journal::Client,
    /// Owns the brokers and etcd shared by this test binary.
    data_plane: e2e_support::DataPlane,
    /// Broker endpoint passed to daemon processes.
    endpoint: String,
    /// Base64 HMAC keys the daemon signs its broker tokens with.
    auth_keys: String,
    /// Interval at which brokers re-list the fragment store.
    refresh_interval_seconds: u64,
}

impl Fixture {
    /// Start a data plane whose brokers rarely re-list the fragment store.
    pub async fn start() -> Self {
        Self::start_refreshing(SLOW_REFRESH_SECONDS).await
    }

    /// [`Fixture::start`], with brokers which re-list the fragment store every
    /// `refresh_interval_seconds`. A case which deletes fragments needs a short one.
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

    /// Assert nothing leaked, then stop the data plane.
    pub async fn stop(self) {
        () = self.assert_no_leaks();

        self.data_plane
            .graceful_stop()
            .await
            .expect("DataPlane graceful_stop");
    }

    /// Spec of a disk journal. Snappy fragments make reads go through broker
    /// decompression. [`Fixture::open`] keeps an existing journal, so a case may first
    /// create one with another spec.
    pub fn spec(&self, journal: &str) -> broker::JournalSpec {
        broker::JournalSpec {
            name: journal.to_string(),
            replication: 1,
            labels: Some(labels::build_set([(
                labels::CONTENT_TYPE,
                disk_daemon::CONTENT_TYPE_DISK,
            )])),
            fragment: Some(broker::journal_spec::Fragment {
                length: 1 << 20, // 1 MiB
                compression_codec: broker::CompressionCodec::Snappy as i32,
                stores: vec!["file:///".to_string()],
                refresh_interval: Some(
                    std::time::Duration::from_secs(self.refresh_interval_seconds).into(),
                ),
                // Only fragment length closes a fragment, so boundaries are predictable.
                flush_interval: Some(std::time::Duration::from_secs(48 * 3600).into()),
                retention: None,
                path_postfix_template: String::new(),
            }),
            flags: broker::journal_spec::Flag::ORdwr as u32,
            max_append_rate: 1 << 22, // 4 MiB/s
            suspend: None,
        }
    }

    /// Create `journal` if absent, and build its `Open`.
    pub async fn open(&self, journal: &str) -> disk_daemon::proto::Open {
        () = self.create_journal(self.spec(journal)).await;

        self.open_absent(journal)
    }

    /// Open and promote `journal` through `client`, with a recovered `ack_intent`.
    pub async fn open_disk(
        &self,
        client: &disk_daemon::client::Client,
        journal: &str,
        ack_intent: bytes::Bytes,
    ) -> (disk_daemon::client::Disk, std::path::PathBuf) {
        client
            .open(self.open(journal).await, ack_intent)
            .await
            .unwrap_or_else(|err| panic!("opening {journal}: {err}"))
    }

    /// `Open` of `journal`, without creating it.
    pub fn open_absent(&self, journal: &str) -> disk_daemon::proto::Open {
        disk_daemon::proto::Open {
            journal: journal.to_string(),
            device_size: DEVICE_SIZE,
        }
    }

    /// Create a journal from `spec`, leaving an existing journal unchanged.
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
        self.listing(journal).await.and_then(|listed| listed.spec)
    }

    /// Listing of `journal`, with the `mod_revision` a conditional Apply needs.
    async fn listing(&self, journal: &str) -> Option<broker::list_response::Journal> {
        let response = self
            .client
            .list(broker::ListRequest {
                selector: Some(broker::LabelSelector {
                    include: Some(labels::build_set([("name", journal)])),
                    exclude: None,
                }),
                ..Default::default()
            })
            .await
            .expect("listing a journal");

        response.journals.into_iter().next()
    }

    /// Broker-confirmed write head. Like any append, this resumes a suspended journal.
    pub async fn head(&self, journal: &str) -> i64 {
        let response = self
            .append_nothing(journal, broker::append_request::Suspend::Resume)
            .await
            .expect("a resuming append is never refused as suspended");

        response.commit.map_or(0, |commit| commit.end)
    }

    /// Each record of `journal` from `offset`, which must be a record boundary, with
    /// the offset it begins at.
    pub async fn records_from(
        &self,
        journal: &str,
        offset: u64,
    ) -> Vec<(u64, disk_daemon::proto::DiskRecord)> {
        // Without an end offset, a non-blocking read fails at the head as not yet
        // available.
        let head = self.head(journal).await;

        let mut content = bytes::BytesMut::new();
        let mut stream = std::pin::pin!(self.client.clone().read(broker::ReadRequest {
            journal: journal.to_string(),
            offset: offset as i64,
            end_offset: head,
            block: false,
            ..Default::default()
        }));

        while let Some(response) = futures::StreamExt::next(&mut stream).await {
            let response = response.expect("reading a journal");
            if response.content.is_empty() {
                continue;
            }
            // The offsets below count from `offset`, so the read must not skip ahead.
            assert_eq!(
                response.offset,
                offset as i64 + content.len() as i64,
                "the read of {journal} skipped content"
            );
            content.extend_from_slice(&response.content);
        }

        let mut records = Vec::new();
        let mut at = offset;

        while !content.is_empty() {
            match proto_gazette::fixed_framing::unpack(&mut content).expect("a record decodes") {
                proto_gazette::fixed_framing::Frame::Record { message, framed } => {
                    records.push((at, message));
                    at += framed.len() as u64;
                }
                frame => panic!("expected a record of {journal} at {at}, got {frame:?}"),
            }
        }
        records
    }

    /// Suspend `journal` with an empty append, as the broker's idle pulse does. A
    /// `Suspended` reply means it worked.
    pub async fn suspend(&self, journal: &str, mode: broker::append_request::Suspend) {
        _ = self.append_nothing(journal, mode).await;
    }

    /// Append nothing, which reports the head. `None` if the broker answers `Suspended`.
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
                // The next poll backs off and retries.
                Err(gazette::RetryError { inner, .. }) if inner.is_transient() => (),
                Err(gazette::RetryError { inner, .. }) => panic!("appending to {journal}: {inner}"),
            }
        }
    }

    /// Suspension recorded on `journal`'s spec: absent if never suspended, and at
    /// level NONE once resumed.
    pub async fn suspension(&self, journal: &str) -> Option<broker::journal_spec::Suspend> {
        self.listed(journal).await.and_then(|spec| spec.suspend)
    }

    /// Read the daemon's recovery floor from the journal label, as a pruner does.
    pub async fn stored_floor(&self, journal: &str) -> Option<u64> {
        let spec = self.listed(journal).await?;
        let set = spec.labels.unwrap_or_default();

        match labels::maybe_one(&set, labels::DISK_REPLAY_FROM).expect("one floor label") {
            "" => None,
            value => Some(u64::from_str_radix(value, 16).expect("a floor label")),
        }
    }

    /// Set or remove the floor label. Unlike the daemon, this may move it backward.
    pub async fn plant_floor(&self, journal: &str, floor: Option<u64>) {
        let listed = self.listing(journal).await.expect("the journal exists");
        let mut spec = listed.spec.expect("a listed journal has a spec");
        let set = spec.labels.take().unwrap_or_default();

        spec.labels = Some(match floor {
            Some(floor) => {
                labels::set_value(set, labels::DISK_REPLAY_FROM, &format!("{floor:016x}"))
            }
            None => labels::remove(set, labels::DISK_REPLAY_FROM),
        });

        // Compare-and-swap, so a concurrent change fails the case instead of being lost.
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

    /// Where the brokers persist fragments.
    pub fn fragment_root(&self) -> &std::path::Path {
        self.data_plane.gazette.fragment_root.as_path()
    }

    /// Signal every broker. `STOP` and `CONT` start and end an outage.
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

    /// Wait for a teardown which no attachment can report, then assert nothing leaked.
    pub async fn wait_for_teardown(&self) {
        let deadline = std::time::Instant::now() + TEARDOWN;

        loop {
            let blocks = entries("/sys/block", |name| name.starts_with("ublkb"));
            if blocks.is_empty() {
                break;
            }
            // On timeout, the daemon's log shows which teardown step stalled. A device
            // it never mentions belongs to another daemon on this host.
            assert!(
                std::time::Instant::now() < deadline,
                "block devices {blocks:?} outlived a teardown of {TEARDOWN:?}",
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        self.assert_no_leaks();
    }

    fn path(&self) -> &str {
        self.dir
            .path()
            .to_str()
            .expect("paths of a tempdir are UTF-8")
    }
}

/// The daemon binary, run as root through `sudo -n` so the cases stay unprivileged.
pub struct Daemon {
    pub uds_path: std::path::PathBuf,
    child: async_process::Child,
}

impl Daemon {
    pub async fn start(fixture: &Fixture, name: &str) -> Self {
        Self::start_with(fixture, name, &[]).await
    }

    /// [`Daemon::start`], with `extra` command-line flags.
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

    /// Connect a client. Its disks share one connection.
    pub async fn client(&self) -> disk_daemon::client::Client {
        disk_daemon::client::Client::connect(&self.uds_path)
            .await
            .expect("connecting the client")
    }

    /// Open a raw attachment stream, for requests [`disk_daemon::client::Disk`] cannot
    /// express: out of turn, malformed, or pipelined.
    pub async fn attach(&self) -> attachment::Attachment {
        attachment::Attachment::open(self.connect().await.expect("connect")).await
    }

    /// Name and kernel flags word of each daemon thread. `/proc` shows these for a root
    /// process to anyone.
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

    /// `SIGTERM` the daemon, and require that its drain exits cleanly.
    pub async fn drain(mut self) {
        () = self.signal("TERM");
        let status = self.child.wait().await.expect("waiting for the daemon");

        assert!(status.success(), "the daemon drained with {status}");
    }

    /// `SIGKILL` the daemon. Kill only a daemon which serves no disk: killing one with
    /// ublk requests in flight can wedge later ublk control commands.
    pub async fn kill(mut self) {
        () = self.signal("KILL");
        _ = self.child.wait().await.expect("waiting for the daemon");
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

    /// Signal the daemon through `sudo`, since it runs as root.
    fn signal(&self, signal: &str) {
        _ = std::process::Command::new("sudo")
            .args(["-n", "pkill", &format!("-{signal}"), "-f"])
            .arg(self.pattern())
            .status()
            .expect("spawning pkill");
    }

    /// Whether the daemon is still running, probed with signal zero.
    fn running(&self) -> bool {
        std::process::Command::new("sudo")
            .args(["-n", "pkill", "-0", "-f"])
            .arg(self.pattern())
            .status()
            .expect("spawning pkill")
            .success()
    }

    /// Pattern of this daemon's command line. It is anchored, so it skips the parent
    /// `sudo`, whose command line contains the same text.
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
        // Clean up even if the case failed. TERM first, since a KILL with ublk
        // requests in flight can wedge the host until reboot.
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

/// Require that an open was refused as invalid, and return its status. `Disk` is not
/// `Debug`, so `expect_err` can't be used.
pub async fn expect_invalid(
    opened: disk_daemon::client::Result<(disk_daemon::client::Disk, std::path::PathBuf)>,
) -> tonic::Status {
    let err = match opened {
        Err(err) => err,
        Ok((disk, mount)) => {
            _ = disk.close().await;
            panic!("the open was not refused: it returned {mount:?}");
        }
    };

    let disk_daemon::client::Error::Invalid(status) = err else {
        panic!("expected an invalid request, got {err:?}");
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");

    status
}

/// Prepare, acknowledge, and await the commit. Returns the ACK intent.
pub async fn commit(disk: &mut disk_daemon::client::Disk) -> bytes::Bytes {
    let ack_intent = prepare(disk).await;

    () = disk.acknowledge().await.expect("acknowledge");
    () = disk.acknowledged().await.expect("the commit landed");

    ack_intent
}

/// Prepare a non-empty delta, and leave it unacknowledged.
pub async fn prepare(disk: &mut disk_daemon::client::Disk) -> bytes::Bytes {
    let ack_intent = disk.prepare().await.expect("prepare");
    assert!(!ack_intent.is_empty(), "the disk changed");

    ack_intent
}

/// Prepare and acknowledge a delta, which must be empty.
pub async fn prepare_nothing(disk: &mut disk_daemon::client::Disk) {
    let ack_intent = disk.prepare().await.expect("prepare");
    assert!(
        ack_intent.is_empty(),
        "the disk changed when nothing wrote to it"
    );

    () = disk.acknowledge().await.expect("acknowledge");
    () = disk
        .acknowledged()
        .await
        .expect("the empty delta was acknowledged");
}

/// Whether anything is mounted at `path`.
pub fn is_mounted(path: &std::path::Path) -> bool {
    let path = path.to_str().expect("paths of a tempdir are UTF-8");

    std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .any(|line| line.split_whitespace().nth(1) == Some(path))
}

/// Wait for the teardown of a dropped attachment to unmount `mount`.
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

/// Wait until writes park at the device mounted at `mount`: some are in flight, and
/// none completes. Writes park once the recording channel is full.
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

/// Run a command as root, and return its stdout.
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

/// Spawn `dd` to write `bytes` random bytes to `path`, so a case can act while the
/// write is in flight. With `sync`, `dd` fsyncs before it exits.
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

/// Wait for a spawned writer to succeed, without blocking the runtime.
pub async fn wait_for_writer(writer: &mut std::process::Child) {
    let status = abandon_writer(writer).await;

    assert!(status.success(), "a write failed: {status}");
}

/// Wait for a spawned writer to exit, successfully or not.
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

/// Fail, rather than skip, on a host which cannot serve ublk devices.
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

/// Assert no ublk device node or `/sys/block` entry remains. This polls briefly, since
/// `devtmpfs` unlinks a node shortly after its device is removed.
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

/// Assert nothing is mounted under `dir` or from a ublk device.
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
