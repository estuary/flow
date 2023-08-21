use std::fmt::Debug;

use anyhow::Context;
use clap::AppSettings;
use clap::Parser;

mod auth;
mod catalog;
mod collection;
mod config;
mod connector;
mod controlplane;
mod dataplane;
mod draft;
mod generate;
mod local_specs;
mod ops;
mod output;
mod pagination;
mod poll;
mod preview;
mod raw;

use output::{Output, OutputType};
use poll::poll_while_queued;

use crate::pagination::into_items;

/// A command-line tool for working with Estuary Flow.
#[derive(Debug, Parser)]
#[clap(
    author,
    about,
    version,
    global_setting = AppSettings::DeriveDisplayOrder
)]
pub struct Cli {
    /// Configuration profile to use.
    ///
    /// Profile are distinct configurations of the `flowctl` tool, and are
    /// completely optional. Use multiple profiles to track multiple accounts
    /// or development endpoints.
    #[clap(long, default_value = "default", env = "FLOWCTL_PROFILE")]
    profile: String,

    #[clap(subcommand)]
    cmd: Command,

    #[clap(flatten)]
    output: Output,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Authenticate with Flow.
    Auth(auth::Auth),
    /// Work with the current Flow catalog.
    Catalog(catalog::Catalog),
    /// Work with Flow collections.
    Collections(collection::Collections),
    /// Generate derivation project files and implementation stubs.
    ///
    /// Generate walks your local Flow catalog source file and its imports
    /// to gather collections, derivations, and associated JSON schemas.
    /// Your derivations generate associated project files and supplemental
    /// type implementations which are then written into your project directory,
    /// which is the top-level directory having a flow.yaml or flow.json file.
    ///
    /// You then edit the generated stubs in your preferred editor to fill
    /// out implementations for your derivation transform lambdas.
    Generate(generate::Generate),
    /// Locally run and preview the output of a derivation.
    ///
    /// Preview runs a temporary, local instance of your derivation by reading
    /// documents from your source collections and applying them to your
    /// derivation's transforms. The output of your derivation is combined and
    /// periodically written to stdout as newline-delimited JSON.
    /// Preview will process all of your historical source data and thereafter
    /// will emit ongoing updates, until you ask it to exit by sending CTRL-D
    /// (which closes stdin).
    ///
    /// Preview is also able to infer and update the schema of your collection,
    /// based on the documents that your transforms are observed to publish.
    Preview(preview::Preview),
    /// Work with your Flow catalog drafts.
    ///
    /// Drafts are in-progress specifications which are not yet "live".
    /// They can be edited, developed, and tested while still a draft.
    /// Then when you're ready, publish your draft to make your changes live.
    Draft(draft::Draft),
    /// This command does not (yet) work for end users
    ///
    /// Note: We're still working on allowing users access to task logs, and this command will not work until we do.
    /// Prints the runtime logs of a task (capture, derivation, or materialization).
    /// Reads contents from the `ops.<data-plane>/logs` collection, selecting the partition
    /// that corresponds to the selected task. This command is essentially equivalent to the much longer:
    /// `flowctl collections read --collection ops.<data-plane>/logs --include-partition estuary.dev/field/name=<task> --uncommitted`
    Logs(ops::Logs),
    /// This command does not (yet) work for end users
    ///
    /// Note: We're still working on allowing users access to task stats, and this command will not work until we do.
    /// Prints the runtime stats of a task (capture, derivation, or materialization).
    /// Reads contents from the `ops.<data-plane>/stats` collection, selecting the partition
    /// that corresponds to the selected task. This command is essentially equivalent to the much longer:
    /// `flowctl collections read --collection ops.<data-plane>/stats --include-partition estuary.dev/field/name=<task>`
    Stats(ops::Stats),
    /// Advanced, low-level, and experimental commands which are less common.
    Raw(raw::Advanced),
}

#[derive(Debug)]
pub struct CliContext {
    config: config::Config,
    output: output::Output,
    controlplane_client: Option<controlplane::Client>,
}

impl CliContext {
    /// Returns a client to the controlplane, creating a new one if necessary.
    /// This function will return an error if the authentication credentials
    /// are missing, invalid, or expired.
    pub async fn controlplane_client(&mut self) -> anyhow::Result<controlplane::Client> {
        if self.controlplane_client.is_none() {
            let client = controlplane::new_client(self).await?;
            self.controlplane_client = Some(client.clone())
        }
        Ok(self.controlplane_client.clone().unwrap())
    }

    pub fn config_mut(&mut self) -> &mut config::Config {
        &mut self.config
    }

    pub fn config(&self) -> &config::Config {
        &self.config
    }

    pub fn output_args(&self) -> &output::Output {
        &self.output
    }

    pub fn write_all<I, T>(&mut self, items: I, table_alt: T::TableAlt) -> anyhow::Result<()>
    where
        T: output::CliOutput,
        I: IntoIterator<Item = T>,
    {
        match self.get_output_type() {
            OutputType::Json => output::print_json(items),
            OutputType::Yaml => output::print_yaml(items),
            OutputType::Table => output::print_table(table_alt, items),
        }
    }

    pub fn get_output_type(&mut self) -> OutputType {
        use crossterm::tty::IsTty;

        if let Some(ty) = self.output.output {
            ty
        } else {
            if std::io::stdout().is_tty() {
                OutputType::Table
            } else {
                OutputType::Yaml
            }
        }
    }
}

impl Cli {
    pub async fn run(&self) -> anyhow::Result<()> {
        let config = config::Config::load(&self.profile)?;
        let output = self.output.clone();
        let mut context = CliContext {
            config,
            output,
            controlplane_client: None,
        };

        match &self.cmd {
            Command::Auth(auth) => auth.run(&mut context).await,
            Command::Catalog(catalog) => catalog.run(&mut context).await,
            Command::Collections(collection) => collection.run(&mut context).await,
            Command::Generate(generate) => generate.run(&mut context).await,
            Command::Preview(preview) => preview.run(&mut context).await,
            Command::Draft(draft) => draft.run(&mut context).await,
            Command::Logs(logs) => logs.run(&mut context).await,
            Command::Stats(stats) => stats.run(&mut context).await,
            Command::Raw(advanced) => advanced.run(&mut context).await,
        }?;

        context.config().write(&self.profile)?;

        Ok(())
    }
}

// api_exec runs a PostgREST request, debug-logs its request, and turns non-success status into an anyhow::Error.
async fn api_exec<T>(b: postgrest::Builder) -> anyhow::Result<T>
where
    for<'de> T: serde::Deserialize<'de>,
{
    let req = b.build();
    tracing::debug!(?req, "built request to execute");

    let resp = req.send().await?;
    let status = resp.status();

    if status.is_success() {
        let v: serde_json::Value = resp.json().await?;

        tracing::trace!(response_body = %v, status = %status, "got successful response");
        let t: T = serde_json::from_value(v).context("deserializing response body")?;
        Ok(t)
    } else {
        let body = resp.text().await?;
        anyhow::bail!("{status}: {body}");
    }
}

/// Execute a [`postgrest::Builder`] request returning multiple rows. Unlike [`api_exec`]
/// which is limited to however many rows Postgrest is configured to return in a single response,
/// this will issue as many paginated requests as necessary to fetch every row.
async fn api_exec_paginated<T>(b: postgrest::Builder) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    use futures::TryStreamExt;

    let pages = into_items(b).try_collect().await?;

    Ok(pages)
}

// new_table builds a comfy_table with UTF8 styling.
fn new_table(headers: Vec<&str>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .apply_modifier(comfy_table::modifiers::UTF8_SOLID_INNER_BORDERS);

    table.set_header(headers);
    table
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct Timestamp(#[serde(with = "time::serde::rfc3339")] time::OffsetDateTime);

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self.0.replace_nanosecond(0).unwrap();
        let ts = ts
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        f.write_str(&ts)
    }
}

impl Timestamp {
    pub fn from_unix_timestamp(epoch_time_seconds: i64) -> Result<Timestamp, anyhow::Error> {
        let offset_date_time = time::OffsetDateTime::from_unix_timestamp(epoch_time_seconds)?;
        Ok(Timestamp(offset_date_time))
    }
}

fn format_user(email: Option<String>, full_name: Option<String>, id: Option<uuid::Uuid>) -> String {
    format!(
        "{full_name} <{email}>\n{id}",
        full_name = full_name.unwrap_or_default(),
        email = email.unwrap_or_default(),
        id = id.map(|id| id.to_string()).unwrap_or_default(),
    )
}
