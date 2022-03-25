pub mod errors;
pub mod firebolt_projections;
pub mod firebolt_queries;
pub mod firebolt_schema_builder;
pub mod firebolt_types;

use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};
use std::iter::FromIterator;

use anyhow::Context;
use firebolt_projections::{
    validate_binding_against_constraints, validate_existing_projection, validate_new_projection,
};
use firebolt_schema_builder::build_firebolt_queries_bundle;
use prost::Message;
use protocol::flow::MaterializationSpec;
use protocol::materialize::{extra, validate_request};

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(subcommand)]
    action: Action,
}

#[derive(clap::Subcommand, Debug)]
enum Action {
    ValidateNewProjection,
    ValidateExistingProjection,
    ValidateBindingAgainstConstraints,
    QueryBundle,
}

pub fn run(args: Args) -> Result<(), anyhow::Error> {
    let mut buf: Vec<u8> = Vec::new();
    io::stdin()
        .read_to_end(&mut buf)
        .context("Failed to read stdin to buffer")?;

    let output = match args.action {
        Action::QueryBundle => {
            let spec = MaterializationSpec::decode(Cursor::new(buf))?;

            let result = build_firebolt_queries_bundle(spec)?;
            serde_json::to_string(&result)?
        }
        Action::ValidateNewProjection => {
            let projection = validate_request::Binding::decode(Cursor::new(buf))?;

            let result = validate_new_projection(projection);
            serde_json::to_string(&result)?
        }
        Action::ValidateExistingProjection => {
            let req = extra::ValidateExistingProjectionRequest::decode(Cursor::new(buf))?;

            let result = validate_existing_projection(
                req.existing_binding.unwrap(),
                req.proposed_binding.unwrap(),
            );
            serde_json::to_string(&result)?
        }
        Action::ValidateBindingAgainstConstraints => {
            let req = extra::ValidateBindingAgainstConstraints::decode(Cursor::new(buf))?;

            let result = validate_binding_against_constraints(
                BTreeMap::from_iter(req.constraints.into_iter()),
                req.binding.unwrap(),
            );
            if result.is_ok() {
                "".to_string()
            } else {
                serde_json::to_string(&result)?
            }
        }
    };

    io::stdout().write_all(output.as_bytes())?;
    Ok(())
}
