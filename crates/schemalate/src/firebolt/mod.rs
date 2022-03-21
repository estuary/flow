pub mod errors;
pub mod firebolt_projections;
pub mod firebolt_queries;
pub mod firebolt_schema_builder;
pub mod firebolt_types;

use std::io::{self, Cursor, Read, Write};

use anyhow::Context;
use firebolt_projections::{validate_existing_projection, validate_new_projection};
use firebolt_schema_builder::build_firebolt_queries_bundle;
use prost::Message;
use protocol::flow::MaterializationSpec;
use protocol::materialize::validate_request;

use self::firebolt_projections::ValidateExistingProjectionRequest;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Validate new projection
    #[clap(long)]
    validate_new_projection: bool,

    /// Validate existing projection
    #[clap(long)]
    validate_existing_projection: bool,

    /// Generate query bundle
    #[clap(long)]
    query_bundle: bool,
}

pub fn run(args: Args) -> Result<(), anyhow::Error> {
    let mut buf: Vec<u8> = Vec::new();
    io::stdin()
        .read_to_end(&mut buf)
        .context("Failed to read stdin to buffer")?;

    let output = if args.query_bundle {
        let spec = MaterializationSpec::decode(Cursor::new(buf))?;

        let result = build_firebolt_queries_bundle(spec)?;
        serde_json::to_string(&result)?
    } else if args.validate_new_projection {
        let projection = validate_request::Binding::decode(Cursor::new(buf))?;

        let result = validate_new_projection(projection);
        serde_json::to_string(&result)?
    } else {
        let req = ValidateExistingProjectionRequest::decode(Cursor::new(buf))?;

        let result = validate_existing_projection(
            req.existing_binding.unwrap(),
            req.proposed_binding.unwrap(),
        );
        serde_json::to_string(&result)?
    };

    io::stdout().write_all(output.as_bytes())?;
    Ok(())
}
