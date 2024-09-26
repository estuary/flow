use std::fmt::Debug;

use anyhow::Context;
use clap::Parser;

mod auth;
mod catalog;
mod client;
mod collection;
mod config;
mod draft;
mod generate;
mod local_specs;
mod ops;
mod output;
pub mod pagination;
mod poll;
mod preview;
mod raw;

pub use client::{fetch_collection_authorization, fetch_task_authorization, Client};
pub use config::Config;
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

        // If the configured access token has expired then remove it before continuing.
        if let Some(token) = &config.user_access_token {
            let claims: models::authorizations::ControlClaims =
                parse_jwt_claims(token).context("failed to parse control-plane access token")?;

            let now = time::OffsetDateTime::now_utc();
            let exp = time::OffsetDateTime::from_unix_timestamp(claims.exp as i64).unwrap();

            if now + std::time::Duration::from_secs(60) > exp {
                tracing::info!(expired=%exp, "removing expired user access token from configuration");
                config.user_access_token = None;
            }
        }

        let mut client = Client::new(&config);

        if config.user_access_token.is_some() || config.user_refresh_token.is_some() {
            client.refresh().await?;
        } else {
            tracing::warn!("You are not authenticated. Run `auth login` to login to Flow.");
        }

        let mut context = CliContext {
            client,
            config,
            output,
        };

        match &self.cmd {
            Command::Auth(auth) => auth.run(&mut context).await,
            Command::Catalog(catalog) => catalog.run(&mut context).await,
            Command::Collections(collection) => collection.run(&mut context).await,
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
        let body: models::RawValue = resp.json().await?;
        tracing::trace!(body = ?::ops::DebugJson(&body), status = %status, "got successful response");
        let t: T = serde_json::from_str(body.get()).context("deserializing response body")?;
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

    let pages = pagination::into_items(b).try_collect().await?;

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

fn parse_jwt_claims<T: serde::de::DeserializeOwned>(token: &str) -> anyhow::Result<T> {
    let claims = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("malformed token"))?;
    let claims = base64::decode_config(claims, base64::URL_SAFE_NO_PAD)?;
    anyhow::Result::Ok(serde_json::from_slice(&claims)?)
}
