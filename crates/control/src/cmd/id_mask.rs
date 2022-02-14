use std::str::FromStr;

use anyhow::anyhow;

use crate::models::Id;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(subcommand)]
    pub mode: Mode,
}

#[derive(Debug, clap::Subcommand)]
pub enum Mode {
    /// Decode an id into a i64
    Decode { encoded_value: String },
    /// Encode an id as a i64
    Encode { raw_value: String },
}

pub fn run(args: Args) -> anyhow::Result<()> {
    match args.mode {
        Mode::Decode { encoded_value } => decode(encoded_value),
        Mode::Encode { raw_value } => encode(raw_value),
    }
}

fn decode(value: String) -> anyhow::Result<()> {
    let id = Id::from_str(&value)
        .map_err(|_| anyhow!("The provided value `{}` does not appear to be an id", value))?;
    println!("{:?}", id);
    Ok(())
}

fn encode(value: String) -> anyhow::Result<()> {
    let i = i64::from_str(&value).map_err(|_| {
        anyhow!(
            "The provided value `{}` does not appear to be an i64",
            value
        )
    })?;
    let id = Id::new(i);
    println!("{}", id);
    Ok(())
}
