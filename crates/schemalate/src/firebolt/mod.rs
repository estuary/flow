pub mod errors;
pub mod firebolt_projections;
pub mod firebolt_schema_builder;
pub mod firebolt_types;

use std::io::{self, Write};

use firebolt_schema_builder::build_firebolt_schema;

pub fn run() -> Result<(), anyhow::Error> {
    let schema = serde_json::from_reader(io::stdin())?;

    let result = build_firebolt_schema(&schema)?;

    io::stdout().write_all(result.to_string().as_bytes())?;
    Ok(())
}
