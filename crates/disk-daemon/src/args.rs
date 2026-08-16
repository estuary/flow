//! The daemon's command line.
//!
//! Two kinds of knob are here, and they differ in what a change may do. Host
//! facts and policy are safe to change between restarts, because a disk's
//! durable state is derived from its journal and never from configuration.
//! Everything which is durable per disk, notably device and block size, is a
//! session's `Open` parameter instead: a flag would otherwise reinterpret every
//! disk on the host at once.
//!
//! Nothing here is a fallback for something a session may also supply. A value
//! has one source, so no precedence rule needs explaining and an omission
//! fails rather than silently resolving to a host default.
//!
//! Every flag also reads an unprefixed environment variable, as `runtime-sidecar`
//! and `dekaf` do. The Go reactor namespaces its own with `FLOW_*` and
//! `CONSUMER_*` because that is gazette `mainboilerplate` convention.

#[derive(Debug, clap::Parser)]
#[command(about, version)]
pub struct Args {
    /// Unix socket the session service listens on.
    #[arg(long, env = "UDS_PATH")]
    pub uds_path: std::path::PathBuf,

    /// Directory a disk's sparse image is created in. A host with several
    /// drives stripes them beneath it rather than naming each one here.
    #[arg(long, env = "IMAGE_DIR")]
    pub image_dir: std::path::PathBuf,

    /// Directory a disk's filesystem is mounted under. The mount path is
    /// returned to the session, which places it into its sandbox.
    #[arg(long, env = "MOUNT_DIR")]
    pub mount_dir: std::path::PathBuf,

    /// When set, serve the admin and metrics surface on `127.0.0.1:<port>`.
    /// Loopback-only: this surface has no authentication, and it can change a
    /// handler's logging level.
    #[arg(long, env = "ADMIN_PORT")]
    pub admin_port: Option<u16>,

    #[arg(long, env = "LOG_FORMAT", default_value = "text")]
    pub log_format: LogFormat,

    /// Label on a disk journal's own spec which the daemon advances to the
    /// recovery floor, and reads back as its replay seek hint. Required, and
    /// without a default: bounded recovery depends on it, and a general-purpose
    /// daemon carries no system's label vocabulary.
    #[arg(long, env = "FLOOR_LABEL")]
    pub floor_label: String,

    /// Journal range above the recovery floor, as a multiple of a disk's live
    /// allocated size, beyond which it opens a recovery horizon. With the copy
    /// ratio it bounds the range a recovery reads.
    #[arg(long, env = "HORIZON_OPEN_RATIO", default_value = "2.0")]
    pub horizon_open_ratio: f64,

    /// Unchanged bytes a delta may copy for each byte it changed, which is what
    /// discharges a horizon over blocks nothing is writing. Journal write
    /// amplification during compaction is at most one plus this.
    #[arg(long, env = "HORIZON_COPY_RATIO", default_value = "0.5")]
    pub horizon_copy_ratio: f64,

    /// Journal range below which no horizon opens, whatever the ratio. It keeps
    /// a small disk from compacting constantly.
    #[arg(long, env = "HORIZON_MINIMUM_BYTES", default_value_t = 1 << 30)]
    pub horizon_minimum_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
