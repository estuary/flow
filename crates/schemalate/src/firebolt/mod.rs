pub mod errors;
pub mod firebolt_projections;
pub mod firebolt_queries;
pub mod firebolt_schema_builder;
pub mod firebolt_types;

use std::io::{self, Cursor, Read, Write};

use anyhow::Context;
use firebolt_schema_builder::build_firebolt_queries_bundle;
use prost::Message;
use protocol::flow::MaterializationSpec;
use serde::Deserialize;

pub fn run() -> Result<(), anyhow::Error> {
    let mut buf: Vec<u8> = Vec::new();
    io::stdin()
        .read_to_end(&mut buf)
        .context("Failed to read stdin to buffer")?;

    let spec = match MaterializationSpec::decode(Cursor::new(buf)) {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    let result = build_firebolt_queries_bundle(spec)?;

    let output = serde_json::to_string(&result)?;
    io::stdout().write_all(output.as_bytes())?;
    Ok(())
}
