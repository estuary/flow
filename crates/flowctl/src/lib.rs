use std::fmt::Debug;

use clap::Parser;

mod auth;
mod catalog;
mod collection;
mod config;
mod discover;
mod draft;
mod generate;
mod local_specs;
mod ops;
mod output;
mod poll;
mod preview;
mod raw;

use flow_client::client::refresh_authorizations;
pub(crate) use flow_client::client::Client;
pub(crate) use flow_client::{api_exec, api_exec_paginated};
use output::{Output, OutputType};
use poll::poll_while_queued;

/// A command-line tool for working with Estuary Flow.
#[derive(Debug, Parser)]
#[command(author, about, version, next_display_order = None)]
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
    /// Discover capture bindings from an endpoint.
    ///
    /// Discover runs a discovery operation against a capture's endpoint,
    /// submitting the job to the control plane which runs it on a data-plane.
    /// The discovered bindings are then written to your local Flow catalog files.
    Discover(discover::Discover),
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
    /// Locally run and preview a capture, derivation, or materialization.
    ///
    /// Preview runs a temporary, local instance of your task.
    /// Capture tasks emit captured data to stdout.
    /// Derivations read documents from your source collections, run them
    /// through your derivation connector, and emit the results to stdout.
    /// Materializations read documents from your source collection and
    /// update the configured endpoint system.
    ///
    /// When reading live collection data, preview will process all of your
    /// historical source data and thereafter will emit ongoing updates.
    /// Or, derivations and materializations may alternatively provide a data
    /// --fixture of collection documents to derive or materialize, which is
    /// useful in automated testing contexts.
    ///
    /// Preview provides various knobs for tuning timeouts, simulating back
    /// pressure, and running multiple sessions to exercise connector resumption
    /// and crash recovery, which may be helpful when developing connectors.
    ///
    /// WARNING: previews of captures and materializations make live changes
    /// to their configured endpoints. Be sure that your task does not conflict
    /// or collide with a live task running in the Flow managed service.
    Preview(preview::Preview),
    /// Work with your Flow catalog drafts.
    ///
    /// Drafts are in-progress specifications which are not yet "live".
    /// They can be edited, developed, and tested while still a draft.
    /// Then when you're ready, publish your draft to make your changes live.
    Draft(draft::Draft),
    /// Read operational logs of your tasks (captures, derivations, and materializations).
    Logs(ops::Logs),
    /// Advanced, low-level, and experimental commands which are less common.
    Raw(raw::Advanced),
}

pub struct CliContext {
    client: Client,
    config: config::Config,
    output: output::Output,
}

impl CliContext {
    fn write_all<I, T>(&mut self, items: I, table_alt: T::TableAlt) -> anyhow::Result<()>
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

    fn get_output_type(&mut self) -> OutputType {
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
        let mut config = config::Config::load(&self.profile)?;
        let output = self.output.clone();

        let anon_client: flow_client::Client = config.build_anon_client();

        let client = match refresh_authorizations(
            &anon_client,
            config.user_access_token.to_owned(),
            config.user_refresh_token.to_owned(),
        )
        .await
        {
            Ok((access, refresh)) => {
                // Make sure to store refreshed tokens back in Config so they get written back to disk
                config.user_access_token = Some(access.to_owned());
                config.user_refresh_token = Some(refresh.to_owned());

                anon_client.with_user_access_token(Some(access))
            }
            Err(err) => {
                tracing::debug!(?err, "Error refreshing credentials");
                tracing::warn!("You are not authenticated. Run `auth login` to login to Flow.");
                anon_client
            }
        };

        let mut context = CliContext {
            client,
            config,
            output,
        };

        match &self.cmd {
            Command::Auth(auth) => auth.run(&mut context).await,
            Command::Catalog(catalog) => catalog.run(&mut context).await,
            Command::Collections(collection) => collection.run(&mut context).await,
            Command::Discover(discover) => discover.run(&mut context).await,
            Command::Generate(generate) => generate.run(&mut context).await,
            Command::Preview(preview) => preview.run(&mut context).await,
            Command::Draft(draft) => draft.run(&mut context).await,
            Command::Logs(logs) => logs.run(&mut context).await,
            Command::Raw(advanced) => advanced.run(&mut context).await,
        }?;

        context.config.write(&self.profile)?;

        Ok(())
    }
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
