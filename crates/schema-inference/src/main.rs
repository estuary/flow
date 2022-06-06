use std::fs::File;
use std::io;
use structopt::StructOpt;

mod parse;

/// schema-inference is a program that parses a JSON document and output a JSON Schema
/// representing the values that were found in the document. This command is in ALPHA and
/// subject to change at any time.
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(long, global = true, default_value = "warn", env = "PARSER_LOG")]
    pub log: String,

    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Parse the given `--file` and print the parsed schema to stdin.
    Parse(ParseArgs),
}

#[derive(Debug, StructOpt)]
pub struct ParseArgs {
    /// Path to a file with the data to parse.
    #[structopt(long = "file", default_value = "")]
    pub file: String,
}

fn main() {
    let args = Args::from_args();

    // Logs are written to stderr in jsonl format. This format is very compatible with Flow's log
    // parsing, which means that they will get forwarded with the proper level and will retain the
    // the structure of fields.
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(args.log.as_str())
        .json()
        // Without this, many fields (including the message) would get nested inside of a `"fields"`
        // object, which just makes parsing more difficult.
        .flatten_event(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        // Using CLOSE span events seems like the best balance between helpfulness and verbosity.
        // Any Spans that are created will only be logged once they're done with (i.e. once a
        // `Future` has been `await`ed). This means that timing information will be recorded for
        // each span, and all fields will have had their values recorded. It also means that there
        // will be only 1 log line per span, so shouldn't be too overwhelming.
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        // Adds info on the current span to each event emitted from within it. This might be a
        // little verbose, but we don't really use many spans so :shrug:
        .with_current_span(true)
        // This stuff just seems too verbose to be worth it.
        .with_span_list(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        // "target" here refers to the rust module path (typically) from which the trace event
        // originated. It's not clear how useful it really is, especially for users of Flow, so I
        // left it disabled for now. But I could also see an argument for including it, so if
        // that's what you're here to do then go for it.
        .with_target(false)
        .init();

    match args.command {
        Command::Parse(parse_args) => parse::file(File::open(parse_args.file.as_str()).unwrap()),
    }
}
