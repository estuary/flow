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
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
