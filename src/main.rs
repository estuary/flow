use parser::{parse, Input, ParseConfig};
use std::fs::File;
use std::io;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(long, global = true, default_value = "warn", env = "PARSER_LOG")]
    pub log: String,

    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    Parse(ParseArgs),
    //Detect(ParseArgs),
    Spec,
}

#[derive(Debug, StructOpt)]
pub struct ParseArgs {
    /// Path to the configuration file to use for the parse operation
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
    // format is used for everythign else too.
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
        Box::new(io::stdin())
    } else {
        // TODO: maybe remove this?
        if config.filename.is_none() {
            config.filename = Some(parse_args.file.clone());
        }
        Box::new(File::open(parse_args.file.as_str()).or_bail("failed to open file"))
    };
    let stdout = io::stdout();
    parse(&config, input, Box::new(stdout)).or_bail("parsing failed");
}

fn do_spec() {
    let mut settings = schemars::gen::SchemaSettings::draft07();
    settings.option_add_null_type = false;
    let generator = schemars::gen::SchemaGenerator::new(settings);
    let schema = generator.into_root_schema_for::<ParseConfig>();
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
