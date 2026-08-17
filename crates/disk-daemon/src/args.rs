//! The daemon's command line.
//!
//! Only host facts and policy are here. Both are safe to change between
//! restarts, because a disk derives its durable state from its journal and never
//! from configuration. Everything which is durable per disk is a session's `Open`
//! parameter instead, notably device size. A flag would otherwise reinterpret
//! every disk on the host at once.
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

    /// Journal holding the disk's durable state. It must already exist: the
    /// daemon never creates one. `gazctl journals apply` is how it is made.
    #[arg(long)]
    pub journal: String,

    /// Logical size of the device. The image is sparse, so this is a capacity and
    /// not an allocation.
    #[arg(long, default_value_t = 10 << 30)]
    pub device_size: u64,

    /// Address of a broker serving the journal.
    #[arg(long, env = "BROKER_ENDPOINT")]
    pub broker_endpoint: String,

    /// Bearer token presented to the broker. Without one the session connects
    /// anonymously. That is correct only against a broker which runs without
    /// authorization.
    #[arg(long, env = "BROKER_CREDENTIAL")]
    pub broker_credential: Option<String>,

    /// Recovery floor a previous session reported, which seeks the replay this
    /// one begins with. A real client persists it; driving a disk by hand means
    /// carrying it forward from the `floor` lines this command prints. Zero
    /// replays from the first fragment the store still holds.
    #[arg(long, default_value_t = 0)]
    pub floor_hint: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
