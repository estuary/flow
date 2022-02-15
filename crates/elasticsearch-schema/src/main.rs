pub mod elastic_search_data_types;
pub mod elastic_search_schema_builder;
pub mod errors;
pub mod interface;

use base64::decode;
use std::io::{self};
use std::process;

use elastic_search_schema_builder::build_elastic_schema_with_overrides;
use interface::Input;

fn main() {
    init_tracing();

    let input: Input = serde_json::from_reader(io::stdin())
        .or_bail("Failed parsing json data streamed in from stdin.");

    let schema_json = decode(input.schema_json_base64).or_bail("Failed to decode schema_json");

    let result = build_elastic_schema_with_overrides(&schema_json, &input.overrides)
        .or_bail("Failed generating elastic search schema based on input.");

    serde_json::to_writer(io::stdout(), &result).or_bail("Failed generating output to stdout.")
}

// TODO: Extract the common logic to a separate crate shared by connectors?
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        // TODO(jixiang): make this controlled by the input from GO side.
        .with_env_filter("warn")
        .json()
        .flatten_event(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_current_span(true)
        .with_span_list(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .init();
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
                tracing::debug!(error_details = ?e, message);
                tracing::error!(error = %e, message);
                process::exit(1);
            }
        }
    }
}
