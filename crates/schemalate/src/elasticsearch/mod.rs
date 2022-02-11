pub mod elastic_search_data_types;
pub mod elastic_search_schema_builder;
pub mod errors;

use std::io;

use elastic_search_data_types::ESTypeOverride;
use elastic_search_schema_builder::build_elastic_schema_with_overrides;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[clap(long = "es-type")]
    type_overrides: Vec<ESTypeOverride>,
}

pub fn run(args: Args) -> Result<(), anyhow::Error> {
    let schema = serde_json::from_reader(io::stdin())?;

    let result = build_elastic_schema_with_overrides(&schema, &args.type_overrides)?;

    serde_json::to_writer(io::stdout(), &result)?;
    Ok(())
}
