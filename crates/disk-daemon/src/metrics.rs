//! What the daemon reports about itself on its `service-kit` surface.
//!
//! There are two scopes, and they answer different questions. A disk's own
//! metrics carry its journal as a label. They say whether that disk is keeping
//! up: how much it appends, how far its recovery range has grown, what its open
//! horizon still owes, and whether the journal is holding its device back. The
//! host's metrics say whether the machine can take another disk.
//!
//! The daemon knows its true footprint exactly. A disk's allocated bitmap is the
//! set of blocks its image occupies. `st_blocks` cannot give the same answer,
//! because ext4 delays allocation. It does not charge a block against the image
//! until writeback, even after the disk has written it.

use std::sync::atomic::{AtomicU64, Ordering};

/// How often the daemon samples host capacity. These are facts about the machine
/// rather than events, so a tick reads them rather than a handler reporting them.
const SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// What one disk's owner reports: what its image holds, what its horizon owes,
/// and whether the journal is holding its device back. Each metric's meaning is
/// its description in `describe`.
pub struct Device {
    pub allocated: Contribution,
    pub horizon_pending: metrics::Gauge,
    pub parked: metrics::Gauge,
    pub stalls: metrics::Counter,
}

/// What one disk's journal writer reports: what it has appended, and where a
/// replay of the journal would now begin and end.
#[derive(Clone)]
pub struct Journal {
    pub appended_records: metrics::Counter,
    pub appended_bytes: metrics::Counter,
    pub publishes: metrics::Counter,
    pub commits: metrics::Counter,
    pub horizons: metrics::Counter,
    pub recovery_range: metrics::Gauge,
    pub floor_seconds: metrics::Gauge,
}

impl Device {
    pub fn new(journal: &str, footprint: &Footprint) -> Self {
        () = describe();

        Self {
            allocated: footprint.contribution(journal),
            horizon_pending: metrics::gauge!("disk_daemon_horizon_pending_blocks", "journal" => journal.to_string()),
            parked: metrics::gauge!("disk_daemon_parked_requests", "journal" => journal.to_string()),
            stalls: metrics::counter!("disk_daemon_admission_stalls", "journal" => journal.to_string()),
        }
    }
}

/// A disk which has ended holds nothing and owes nothing. Its gauges must say
/// so, rather than stand at whatever they last reported. Counters are cumulative,
/// so this leaves them alone.
impl Drop for Device {
    fn drop(&mut self) {
        self.horizon_pending.set(0.0);
        self.parked.set(0.0);
    }
}

impl Journal {
    pub fn new(journal: &str) -> Self {
        () = describe();

        Self {
            appended_records: metrics::counter!("disk_daemon_appended_records", "journal" => journal.to_string()),
            appended_bytes: metrics::counter!("disk_daemon_appended_bytes", "journal" => journal.to_string()),
            publishes: metrics::counter!("disk_daemon_publishes", "journal" => journal.to_string()),
            commits: metrics::counter!("disk_daemon_commits", "journal" => journal.to_string()),
            horizons: metrics::counter!("disk_daemon_horizons_completed", "journal" => journal.to_string()),
            recovery_range: metrics::gauge!("disk_daemon_recovery_range_bytes", "journal" => journal.to_string()),
            floor_seconds: metrics::gauge!("disk_daemon_floor_seconds", "journal" => journal.to_string()),
        }
    }
}

/// Bytes every live disk of the host holds. Each disk adds its own share for as
/// long as it exists.
#[derive(Clone, Default)]
pub struct Footprint(std::sync::Arc<AtomicU64>);

/// One disk's share of the host's [`Footprint`].
pub struct Contribution {
    host: std::sync::Arc<AtomicU64>,
    disk: metrics::Gauge,
    held: u64,
}

impl Footprint {
    fn contribution(&self, journal: &str) -> Contribution {
        Contribution {
            host: self.0.clone(),
            disk: metrics::gauge!("disk_daemon_allocated_bytes", "journal" => journal.to_string()),
            held: 0,
        }
    }
}

impl Contribution {
    pub fn set(&mut self, bytes: u64) {
        self.disk.set(bytes as f64);

        if bytes >= self.held {
            self.host.fetch_add(bytes - self.held, Ordering::Relaxed);
        } else {
            self.host.fetch_sub(self.held - bytes, Ordering::Relaxed);
        }
        self.held = bytes;
    }
}

impl Drop for Contribution {
    fn drop(&mut self) {
        self.host.fetch_sub(self.held, Ordering::Relaxed);
        self.disk.set(0.0);
    }
}

/// Sample host capacity until `draining` fires.
///
/// A scrape of these against the image filesystem's free space says whether the
/// host can take another disk, which no per-disk metric can.
pub async fn host(
    image_dir: std::path::PathBuf,
    control: std::sync::Arc<crate::ublk::Control>,
    footprint: Footprint,
    draining: tokio_util::sync::CancellationToken,
) {
    () = describe();

    let allocated = metrics::gauge!("disk_daemon_host_allocated_bytes");
    let free = metrics::gauge!("disk_daemon_image_dir_free_bytes");
    let devices = metrics::gauge!("disk_daemon_devices");
    let devices_max = metrics::gauge!("disk_daemon_devices_max");

    while !crate::daemon::ticked(&draining, SAMPLE_INTERVAL).await {
        allocated.set(footprint.0.load(Ordering::Relaxed) as f64);
        devices.set(control.live() as f64);

        if let Some(max) = control.ublks_max() {
            devices_max.set(max as f64);
        }
        match free_bytes(&image_dir) {
            Ok(bytes) => free.set(bytes as f64),
            Err(err) => tracing::warn!(?image_dir, ?err, "failed to read free space"),
        }
    }
}

/// Bytes an unprivileged writer may still add to the filesystem holding `dir`.
fn free_bytes(dir: &std::path::Path) -> std::io::Result<u64> {
    let path = std::ffi::CString::new(std::os::unix::ffi::OsStrExt::as_bytes(dir.as_os_str()))
        .map_err(std::io::Error::other)?;

    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };

    // SAFETY: `path` is NUL-terminated and outlives the call, which writes only
    // to `stat`.
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

fn describe() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();

    DESCRIBE.call_once(|| {
        metrics::describe_gauge!(
            "disk_daemon_allocated_bytes",
            metrics::Unit::Bytes,
            "bytes of the image a disk's blocks occupy",
        );
        metrics::describe_gauge!(
            "disk_daemon_horizon_pending_blocks",
            metrics::Unit::Count,
            "blocks which still owe the disk's open recovery horizon a copy",
        );
        metrics::describe_gauge!(
            "disk_daemon_parked_requests",
            metrics::Unit::Count,
            "device requests waiting on capture capacity or a closed admission",
        );
        metrics::describe_counter!(
            "disk_daemon_admission_stalls",
            metrics::Unit::Count,
            "mutations the capture channel or a publication's cut refused",
        );
        metrics::describe_counter!(
            "disk_daemon_appended_records",
            metrics::Unit::Count,
            "journal records appended by the disk's writer",
        );
        metrics::describe_counter!(
            "disk_daemon_appended_bytes",
            metrics::Unit::Bytes,
            "framed record bytes appended by the disk's writer",
        );
        metrics::describe_counter!(
            "disk_daemon_publishes",
            metrics::Unit::Count,
            "deltas cut and acknowledged, excluding transactions which changed nothing",
        );
        metrics::describe_counter!(
            "disk_daemon_commits",
            metrics::Unit::Count,
            "acknowledgements appended and confirmed by the broker",
        );
        metrics::describe_counter!(
            "disk_daemon_horizons_completed",
            metrics::Unit::Count,
            "recovery horizons discharged, each of which advanced the floor",
        );
        metrics::describe_gauge!(
            "disk_daemon_recovery_range_bytes",
            metrics::Unit::Bytes,
            "journal bytes above the recovery floor, which a recovery must read",
        );
        metrics::describe_gauge!(
            "disk_daemon_floor_seconds",
            metrics::Unit::Seconds,
            "wall-clock second of the recovery floor a replay seeks from",
        );
        metrics::describe_gauge!(
            "disk_daemon_host_allocated_bytes",
            metrics::Unit::Bytes,
            "bytes every live disk of this host holds",
        );
        metrics::describe_gauge!(
            "disk_daemon_image_dir_free_bytes",
            metrics::Unit::Bytes,
            "free space of the filesystem holding this host's disk images",
        );
        metrics::describe_gauge!(
            "disk_daemon_devices",
            metrics::Unit::Count,
            "ublk devices this daemon is serving",
        );
        metrics::describe_gauge!(
            "disk_daemon_devices_max",
            metrics::Unit::Count,
            "the kernel's ublks_max, which counts unprivileged devices only",
        );
    });
}

#[cfg(test)]
mod test {
    use super::Footprint;

    /// The host's footprint is the sum of its disks'. A disk which ends gives
    /// its share back.
    #[test]
    fn test_disks_add_and_return_their_share() {
        let footprint = Footprint::default();
        let (mut one, mut two) = (
            footprint.contribution("acmeCo/disk/one"),
            footprint.contribution("acmeCo/disk/two"),
        );

        one.set(4096);
        two.set(8192);
        assert_eq!(
            footprint.0.load(std::sync::atomic::Ordering::Relaxed),
            12288
        );

        // A disk which discarded blocks reports less than it did.
        one.set(1024);
        assert_eq!(footprint.0.load(std::sync::atomic::Ordering::Relaxed), 9216);

        drop(two);
        assert_eq!(footprint.0.load(std::sync::atomic::Ordering::Relaxed), 1024);

        drop(one);
        assert_eq!(footprint.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
