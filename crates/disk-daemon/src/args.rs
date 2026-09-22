//! The daemon's command line.

#[derive(Debug, clap::Parser)]
#[command(about, version)]
pub struct Args {
    /// Unix socket the tenure service listens on.
    #[arg(long, env = "UDS_PATH")]
    pub uds_path: std::path::PathBuf,

    /// Brokers serving every disk journal this daemon opens.
    #[arg(long, env = "BROKER_ADDRESS")]
    pub broker_address: String,

    /// Availability zone within which this daemon is running. Broker route
    /// selection prefers a replica of this zone, and a disk's replay is what
    /// reads from one. A replay reads far more than a live disk appends, so a
    /// zone which names nothing costs egress on the larger side. Appends go to
    /// a journal's primary whatever this says.
    #[arg(long, env = "GAZETTE_ZONE", default_value = "local")]
    pub gazette_zone: String,

    /// FQDN of the data plane whose key signs this daemon's broker tokens, stamped
    /// as the `iss` of each one.
    #[arg(long, env = "DATA_PLANE_FQDN")]
    pub data_plane_fqdn: String,

    /// Base64 HMAC keys of the data plane, comma- or whitespace-separated. The first
    /// signs. The daemon mints its own broker tokens rather than being handed one,
    /// as a reactor does for its recovery logs.
    #[arg(long, env = "DATA_PLANE_AUTH_KEYS")]
    pub data_plane_auth_keys: String,

    /// Directory a disk's sparse image is created in.
    #[arg(long, env = "IMAGE_DIR")]
    pub image_dir: std::path::PathBuf,

    /// Directory a disk's filesystem is mounted under.
    #[arg(long, env = "MOUNT_DIR")]
    pub mount_dir: std::path::PathBuf,

    #[arg(long, env = "LOG_FORMAT", default_value = "text")]
    pub log_format: LogFormat,

    /// Journal range above the recovery floor beyond which a disk opens a
    /// recovery horizon, as a multiple of that disk's live allocated size.
    /// Together with the copy ratio it bounds the range a recovery reads.
    #[arg(long, env = "HORIZON_OPEN_RATIO", default_value = "2.0")]
    pub horizon_open_ratio: f64,

    /// Unchanged bytes a delta may copy for each byte it changed. These copies
    /// discharge a horizon over blocks nothing is writing. Journal write
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
