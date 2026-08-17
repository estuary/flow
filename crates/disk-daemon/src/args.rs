//! The daemon's command line.
//!
//! Only host facts and policy are here. Both are safe to change between
//! restarts, because a disk derives its durable state from its journal and never
//! from configuration. Everything which is durable per disk is a session's `Open`
//! parameter instead, notably device and block size. A flag would otherwise
//! reinterpret every disk on the host at once.
//!
//! Every flag also reads an unprefixed environment variable, as
//! `runtime-sidecar` and `dekaf` do.

#[derive(Debug, clap::Parser)]
#[command(about, version)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Serve disks over a Unix socket.
    Serve(Serve),
    /// Drive one session from a terminal or a script.
    Client(Client),
}

#[derive(Debug, clap::Args)]
pub struct Serve {
    /// Unix socket the session service listens on.
    ///
    /// It is left reachable by any user. The permissions of the directory which
    /// holds it decide who may open a session.
    #[arg(long, env = "UDS_PATH")]
    pub uds_path: std::path::PathBuf,

    /// Directory a disk's sparse image is created in. A host with several drives
    /// stripes them beneath it rather than naming each one here.
    #[arg(long, env = "IMAGE_DIR")]
    pub image_dir: std::path::PathBuf,

    /// Directory a disk's filesystem is mounted under. The session receives the
    /// mount path and places it into its sandbox.
    #[arg(long, env = "MOUNT_DIR")]
    pub mount_dir: std::path::PathBuf,

    /// When set, serve the admin and metrics surface on `127.0.0.1:<port>`. It
    /// is loopback-only, because it has no authentication and it can change a
    /// handler's logging level.
    #[arg(long, env = "ADMIN_PORT")]
    pub admin_port: Option<u16>,

    #[arg(long, env = "LOG_FORMAT", default_value = "text")]
    pub log_format: LogFormat,

    /// Label on a disk journal's own spec which the daemon advances to the
    /// recovery floor, and reads back as its replay seek hint. It has no
    /// default, because a general-purpose daemon carries no system's label
    /// vocabulary.
    #[arg(long, env = "FLOOR_LABEL")]
    pub floor_label: String,

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

/// One session, driven from stdin.
///
/// This is the daemon's manual-testing and demo surface, so unlike [`Serve`] it
/// carries defaults for what a session must otherwise supply.
#[derive(Debug, clap::Args)]
pub struct Client {
    /// Unix socket of the daemon to open the disk on.
    #[arg(long, env = "UDS_PATH")]
    pub uds_path: std::path::PathBuf,

    /// Journal holding the disk's durable state, created if it does not exist.
    #[arg(long)]
    pub journal: String,

    /// Logical size of the device. The image is sparse, so this is a capacity and
    /// not an allocation.
    #[arg(long, default_value_t = 10 << 30)]
    pub device_size: u64,

    /// Block size, which is fixed for the life of the disk from its first
    /// publication.
    #[arg(long, default_value_t = 4096)]
    pub block_size: u32,

    /// Address of a broker serving the journal.
    #[arg(long, env = "BROKER_ENDPOINT")]
    pub broker_endpoint: String,

    /// Bearer token presented to the broker. Without one the session connects
    /// anonymously. That is correct only against a broker which runs without
    /// authorization.
    #[arg(long, env = "BROKER_CREDENTIAL")]
    pub broker_credential: Option<String>,

    /// Store of the journal's fragments. Repeat for several.
    #[arg(long, required = true)]
    pub fragment_store: Vec<String>,

    #[arg(long, default_value_t = 1)]
    pub replication: u32,

    /// `name=value` label of the created spec. Repeat for several.
    #[arg(long, value_parser = parse_label)]
    pub label: Vec<(String, String)>,

    #[arg(long, default_value_t = 1 << 26)]
    pub fragment_length: i64,

    /// Interval at which an open fragment is closed and persisted whatever its
    /// size. Zero closes one on size alone.
    #[arg(long, default_value_t = 3600)]
    pub flush_interval_seconds: u32,

    #[arg(long, default_value_t = 300)]
    pub refresh_interval_seconds: u32,

    /// Ceiling on the journal's sustained append rate. Zero is no ceiling.
    #[arg(long, default_value_t = 0)]
    pub max_append_rate: i64,

    /// Codec fragments are compressed with, spelled as Gazette's own enum spells
    /// it. A codec the daemon cannot decompress is rejected.
    #[arg(long, default_value = "SNAPPY", value_parser = parse_codec)]
    pub compression_codec: proto_gazette::broker::CompressionCodec,
}

fn parse_codec(arg: &str) -> Result<proto_gazette::broker::CompressionCodec, String> {
    proto_gazette::broker::CompressionCodec::from_str_name(arg)
        .filter(|codec| gazette::journal::read::supports_codec(*codec))
        .ok_or_else(|| format!("{arg:?} is not a codec this daemon can decompress"))
}

fn parse_label(arg: &str) -> Result<(String, String), String> {
    let (name, value) = arg
        .split_once('=')
        .ok_or_else(|| format!("label {arg:?} is not `name=value`"))?;

    Ok((name.to_string(), value.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
