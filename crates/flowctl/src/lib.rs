use std::fmt::Debug;

use anyhow::Context;
use clap::Parser;
use proto_flow::flow;

mod auth;
mod catalog;
mod config;
mod draft;
mod poll;
mod raw;
mod source;
mod typescript;

use poll::poll_while_queued;

/// A command-line tool for working with Estuary Flow.
#[derive(Debug, Parser)]
#[clap(author, about, version)]
pub struct Cli {
    /// Configuration profile to use.
    ///
    /// Profile are distinct configurations of the `flowctl` tool, and are
    /// completely optional. Use multiple profiles to track multiple accounts
    /// or development endpoints.
    #[clap(long, default_value = "default")]
    profile: String,
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Authenticate with Flow.
    Auth(auth::Auth),
    /// Work with the current Flow catalog.
    Catalog(catalog::Catalog),
    /// Work with your Flow catalog drafts.
    ///
    /// Drafts are in-progress specifications which are not yet "live".
    /// They can be edited, developed, and tested while still a draft.
    /// Then when you're ready, publish your draft to make your changes live.
    Draft(draft::Draft),
    /// Develop TypeScript modules of your local Flow catalog source files.
    Typescript(typescript::TypeScript),
    /// Advanced and low-level commands which are less common.
    Raw(raw::Advanced),
}

impl Cli {
    pub async fn run(&self) -> anyhow::Result<()> {
        let config_dir = dirs::config_dir()
            .context("couldn't determine user config directory")?
            .join("flowctl");
        std::fs::create_dir_all(&config_dir).context("couldn't create user config directory")?;

        let config_file = config_dir.join(format!("{}.json", &self.profile));

        let mut config = config::Config::default();
        match std::fs::read(&config_file) {
            Ok(v) => {
                config = serde_json::from_slice(&v).context("parsing config")?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Pass.
            }
            Err(err) => {
                Err(err).context("opening config")?;
            }
        }

        match &self.cmd {
            Command::Auth(auth) => auth.run(&mut config).await,
            Command::Catalog(catalog) => catalog.run(&mut config).await,
            Command::Draft(draft) => draft.run(&mut config).await,
            Command::Typescript(typescript) => typescript.run(&mut config).await,
            Command::Raw(advanced) => advanced.run(&mut config).await,
        }?;

        std::fs::write(&config_file, &serde_json::to_vec(&config).unwrap())
            .context("writing config")?;

        Ok(())
    }
}

// api_exec runs a PostgREST request, debug-logs its request, and turns non-success status into an anyhow::Error.
async fn api_exec<T>(b: postgrest::Builder<'_>) -> anyhow::Result<T>
where
    for<'de> T: serde::Deserialize<'de>,
{
    let req = b.build();
    tracing::debug!(?req, "built request to execute");

    let resp = req.send().await?;
    let status = resp.status();

    if status.is_success() {
        let v: T = resp.json().await?;
        Ok(v)
    } else {
        let body = resp.text().await?;
        anyhow::bail!("{status}: {body}");
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

/// Fetcher fetches resource URLs from the local filesystem or over the network.
struct Fetcher;

impl sources::Fetcher for Fetcher {
    fn fetch<'a>(
        &'a self,
        // Resource to fetch.
        resource: &'a url::Url,
        // Expected content type of the resource.
        content_type: flow::ContentType,
    ) -> sources::FetchFuture<'a> {
        tracing::debug!(%resource, ?content_type, "fetching resource");
        let url = resource.clone();
        Box::pin(fetch_async(url))
    }
}

async fn fetch_async(resource: url::Url) -> Result<bytes::Bytes, anyhow::Error> {
    match resource.scheme() {
        "http" | "https" => {
            let resp = reqwest::get(resource.as_str()).await?;
            let status = resp.status();

            if status.is_success() {
                Ok(resp.bytes().await?)
            } else {
                let body = resp.text().await?;
                anyhow::bail!("{status}: {body}");
            }
        }
        "file" => {
            let path = resource
                .to_file_path()
                .map_err(|err| anyhow::anyhow!("failed to convert file uri to path: {:?}", err))?;

            let bytes =
                std::fs::read(path).with_context(|| format!("failed to read {resource}"))?;
            Ok(bytes.into())
        }
        _ => Err(anyhow::anyhow!(
            "cannot fetch unsupported URI scheme: '{resource}'"
        )),
    }
}

#[derive(serde::Deserialize, Debug)]
struct Timestamp(#[serde(with = "time::serde::rfc3339")] time::OffsetDateTime);

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self.0.replace_nanosecond(0).unwrap();
        let ts = ts
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        f.write_str(&ts)
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
