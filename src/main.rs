use parser::{parse, Input, ParseConfig};
use std::fs::File;
use std::io;
use std::mem::ManuallyDrop;
use std::ops::DerefMut;
use std::os::unix::io::FromRawFd;
use structopt::StructOpt;

/// parser is a program that parses a variety of formats and emits records in jsonl format.
/// Data can be passed either as a
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(long, global = true, default_value = "warn", env = "PARSER_LOG")]
    pub log: String,

    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Parse the given `--file` (stdin by default) and print the parsed records in jsonl format.
    Parse(ParseArgs),
    /// Prints a JSON schema of the configuration file.
    Spec,
}

#[derive(Debug, StructOpt)]
pub struct ParseArgs {
    /// Path to the configuration file to use for the parse operation. Run the `spec` subcommand to
    /// see the JSON schema of the config file, which includes descriptions of all the fields.
    #[structopt(long = "config-file", env = "PARSE_CONFIG_FILE")]
    pub config_file: Option<String>,

    /// Path to a file with the data to parse. Defaults to '-', which represents stdin.
    /// Passing a value other that '-' will default the filename in the config to the given file.
    /// Some formats, like Excel files, can't really be parsed in a single pass. You need to be
    /// able to seek around the file. This option enables those files to be passed as files, which
    /// allows the parser to avoid duplicating the work of writing the stream to a temporary file.
    /// Note that that's not actually implemented yet, but that's the intent of this option.
    #[structopt(long = "file", default_value = "-")]
    pub file: String,
}

fn main() {
    let args = Args::from_args();

    // Logs are written to stderr in jsonl format, which seems appropriate given that the same
    // format is used for parse output.
    tracing_subscriber::fmt()
        .json()
        .with_span_list(false) // excludes the "spans" array from the output
        // causes span events to be written only after the span closes, which will include timing
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_writer(io::stderr)
        .with_env_filter(args.log.as_str())
        .init();

    match args.command {
        Command::Parse(parse_args) => {
            do_parse(&parse_args);
        }
        Command::Spec => do_spec(),
    }
}

#[tracing::instrument]
fn do_parse(parse_args: &ParseArgs) {
    let mut config = parse_args
        .config_file
        .as_ref()
        .map(|file| ParseConfig::load(file).or_bail("failed to load config file"))
        .unwrap_or_default();
    let input: Input = if parse_args.file == "-" {
        Input::Stream(Box::new(io::stdin()))
    } else {
        if config.filename.is_none() {
            config.filename = Some(parse_args.file.clone());
        }
        Input::File(File::open(parse_args.file.as_str()).or_bail("failed to open file"))
    };
    // Rust's normal Stdout is line buffered and uses a mutex. We don't want any of that, so this
    // creates a plain unbuffered writer from the raw file descriptor, which the internet assures
    // me will always be 1. The ManuallyDrop is critical here, because you *can* close stdout,
    // which would happen automatically when a File is dropped.
    let mut stdout = ManuallyDrop::new(unsafe { File::from_raw_fd(1) });
    parse(&config, input, stdout.deref_mut()).or_bail("parsing failed");
}

fn do_spec() {
    let mut schema = ParseConfig::json_schema();
    // Add a UUID as the $id of the schema. This allows the resulting schema to be nested within
    // other schemas, since any $ref uris will be resolved relative to the $id.
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(meta) = schema.schema.metadata.as_mut() {
        meta.id = Some(id);
    } else {
        unreachable!("schema should always have metadata");
    }
    serde_json::to_writer_pretty(io::stdout(), &schema).or_bail("failed to write schema");
}

trait Must<T> {
    fn or_bail(self, message: &str) -> T;
}

impl<T, E> Must<T> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    fn or_bail(self, message: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!("Error details: {:#?}", e);
                tracing::error!("{}: {}", message, e);
                std::process::exit(1);
            }
        }
    }
}
