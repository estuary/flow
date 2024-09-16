//! Common logging setup code, which can be shared by all rust-based CLIs.

/// Configures logging for all Flow subcommands. These arguments match those that are used by
/// Gazette and the go-based flowctl, so that there's consistency across all our CLIs.
#[derive(Debug, clap::Args)]
pub struct LogArgs {
    /// The log verbosity. Can be one of trace|debug|info|warn|error|off
    #[arg(
        long = "log.level",
        default_value_t = LogLevel::Warn,
        group = "logging",
        ignore_case = true,
        value_enum,
        global = true
    )]
    pub level: LogLevel,

    #[arg(long = "log.format", value_enum, global = true, group = "logging")]
    pub format: Option<LogFormat>,
}

#[derive(Debug, clap::ValueEnum, Clone, Copy)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl ToString for LogLevel {
    fn to_string(&self) -> String {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
        .to_string()
    }
}

/// The format for logs.
#[derive(Debug, clap::ValueEnum, Clone, Copy)]
pub enum LogFormat {
    /// Logs are written to stderr in jsonl format. This format is very compatible with Flow's log
    /// parsing, which means that they will get forwarded with the proper level and will retain the
    /// the structure of fields.
    Json,
    /// Plain text with no colors.
    Text,
    /// Same as plain text, but with fancy colors for better readability in interactive terminals.
    Color,
}

fn default_log_format() -> LogFormat {
    if atty::is(atty::Stream::Stderr) {
        LogFormat::Color
    } else {
        // If running non-interactively, default to JSON so that programatic users don't have to
        // always specify that.
        LogFormat::Json
    }
}

/// Initializes logging, using the given args. Panics if called twice.
pub fn init_logging(args: &LogArgs) {
    let builder = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(&args.level.to_string())
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        // Using CLOSE span events seems like the best balance between helpfulness and verbosity.
        // Any Spans that are created will only be logged once they're done with (i.e. once a
        // `Future` has been `await`ed). This means that timing information will be recorded for
        // each span, and all fields will have had their values recorded. It also means that there
        // will be only 1 log line per span, so shouldn't be too overwhelming.
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_thread_ids(false)
        .with_thread_names(false)
        // "target" here refers to the rust module path (typically) from which the trace event
        // originated. It's not clear how useful it really is, especially for users of Flow, so I
        // left it disabled for now. But I could also see an argument for including it, so if
        // that's what you're here to do then go for it.
        .with_target(false);

    match args.format.unwrap_or_else(default_log_format) {
        LogFormat::Json => {
            builder
                .json()
                .flatten_event(true)
                // Adds info on the current span to each event emitted from within it. This might be a
                // little verbose, but we don't really use many spans so :shrug:
                .with_current_span(true)
                // This stuff just seems too verbose to be worth it.
                .with_span_list(false)
                .init();
        }
        LogFormat::Text => {
            builder.compact().with_ansi(false).init();
        }
        LogFormat::Color => {
            builder.compact().with_ansi(true).init();
        }
    }
}
