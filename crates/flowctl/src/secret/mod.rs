//! `flowctl secret`: the user-facing surface of first-class secrets.
//!
//! A secret is wrapped and stored in two separable steps -- config-encryption
//! `/secret/encrypt` produces the sops document, and the `setSecret` mutation
//! stores it -- which is what lets `--wrapped` re-apply a document that a
//! GitOps repository already holds.

use crate::graphql::*;
use crate::output::{self, JsonCell, to_table_row};
use anyhow::Context;
use std::io::Read;

/// The wrapped sops document of a secret, opaque to everything but
/// config-encryption. Named for the `SecretDocument` graphql scalar.
pub type SecretDocument = models::RawValue;

#[derive(Debug, clap::Args)]
pub struct Secrets {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Set the value of a secret, creating it if it doesn't exist.
    ///
    /// The value is read from stdin, or from `--from-file`, and is never taken
    /// as an argument: arguments land in shell history and process listings.
    /// It's interpreted as a JSON string unless `--json` is given, so a
    /// password of `12345` or `{oops}` keeps its type.
    ///
    /// Setting is idempotent on the document rather than on the value: sops
    /// encryption is non-deterministic, so re-running `set` with an unchanged
    /// value still mints a new secret id. Re-applying a stored document with
    /// `--wrapped` does not.
    Set(SetArgs),
    /// Decrypt and print the value of a secret.
    ///
    /// Requires the `DecryptSecret` capability, which is distinct from the
    /// `ViewSecret` capability that `list` requires.
    Decrypt(DecryptArgs),
    /// List secrets you can view, with their current secret ids.
    List(ListArgs),
    /// Delete a secret by name, or every secret under a prefix.
    Delete(DeleteArgs),
}

#[derive(Debug, clap::Args)]
pub struct SetArgs {
    /// Catalog name of the secret, which must be a sibling of the tasks
    /// which use it: `acmeCo/widgets/token` serves `acmeCo/widgets/rollups`.
    pub name: String,
    /// Read the value from this file instead of stdin.
    #[clap(long, value_name = "FILE")]
    pub from_file: Option<std::path::PathBuf>,
    /// Parse the input as JSON, rather than taking it as a JSON string.
    ///
    /// Use this for structured credentials, such as a service-account
    /// document, which a task merges into an object-shaped location.
    #[clap(long)]
    pub json: bool,
    /// The input is an already-wrapped sops document: store it verbatim.
    ///
    /// This is the GitOps re-apply path. An unchanged document is a no-op
    /// which keeps the secret's current id.
    #[clap(long, conflicts_with = "json")]
    pub wrapped: bool,
}

#[derive(Debug, clap::Args)]
pub struct DecryptArgs {
    /// Catalog name of the secret to decrypt.
    pub name: String,
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// List only secrets under this catalog prefix.
    pub prefix: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct DeleteArgs {
    /// Catalog name of the secret, or -- with `--recursive` -- the prefix
    /// under which every secret is deleted.
    pub name: String,
    /// Treat `name` as a prefix and delete every secret beneath it.
    #[clap(long)]
    pub recursive: bool,
}

impl Secrets {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::Set(args) => do_set(args, ctx).await,
            Command::Decrypt(args) => do_decrypt(args, ctx).await,
            Command::List(args) => do_list(args, ctx).await,
            Command::Delete(args) => do_delete(args, ctx).await,
        }
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/secret/list-query.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone,Debug"
)]
struct ListSecrets;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/secret/set-mutation.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone,Debug"
)]
struct SetSecret;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/secret/delete-mutation.graphql",
    response_derives = "Serialize,Clone,Debug",
    variables_derives = "Clone,Debug"
)]
struct DeleteSecret;

const PAGE_SIZE: i64 = 100;

async fn do_set(args: &SetArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let input = read_input(args.from_file.as_deref())?;

    let document = if args.wrapped {
        models::RawValue::from_string(input).context("provided document is not valid JSON")?
    } else {
        let value = if args.json {
            models::RawValue::from_string(input).context("value is not valid JSON")?
        } else {
            // A bare input is a string value, whatever it looks like.
            models::RawValue::from_value(&serde_json::Value::String(input.trim_end().to_string()))
        };
        wrap_secret(ctx, &args.name, &value).await?
    };

    let response = post_graphql::<SetSecret>(
        &ctx.rest,
        ctx.access_token().as_deref(),
        set_secret::Variables {
            catalog_name: models::Name::new(&args.name),
            document,
        },
    )
    .await
    .context("failed to set secret")?;

    ctx.write_all([response.set_secret], ())
}

/// Wrap `value` into a sops document through config-encryption. The route is
/// unauthenticated: wrapping consumes only a value the caller already holds,
/// and `setSecret` is where authority is enforced.
async fn wrap_secret(
    ctx: &crate::CliContext,
    name: &str,
    value: &models::RawValue,
) -> anyhow::Result<SecretDocument> {
    let client = ctx.config_encryption_client();

    let mut url = client
        .base_url
        .join("/secret/encrypt")
        .expect("path must be valid to join");
    url.query_pairs_mut().append_pair("name", name);

    let response = client
        .http_client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(value.get().to_string())
        .send()
        .await
        .context("failed to reach the config-encryption service")?;

    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("failed to encrypt secret '{name}': {status}: {body}");
    }
    models::RawValue::from_string(body).context("config-encryption returned invalid JSON")
}

async fn do_decrypt(args: &DecryptArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let decryption = tokens::fetch_once(flow_client_next::workflows::UserSecretDecrypt {
        client: ctx.config_encryption_client(),
        user_tokens: ctx.user_tokens.clone(),
        name: models::Secret::new(&args.name),
    })
    .await
    .with_context(|| format!("failed to decrypt secret '{}'", args.name))?;

    let value = decryption
        .value
        .expect("a successful decryption has a value");

    // A string value prints raw, so that piping it into another command does
    // the obvious thing. Anything else prints as JSON.
    match serde_json::from_str::<String>(value.get()) {
        Ok(string) => println!("{string}"),
        Err(_) => println!("{}", value.get()),
    }
    Ok(())
}

async fn do_list(args: &ListArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let filter = args
        .prefix
        .as_ref()
        .map(|prefix| list_secrets::SecretsFilter {
            catalog_name: Some(list_secrets::PrefixFilter {
                starts_with: Some(prefix.clone()),
                in_: None,
            }),
        });

    let mut rows = Vec::new();
    let mut after: Option<String> = None;

    loop {
        let response = post_graphql::<ListSecrets>(
            &ctx.rest,
            ctx.access_token().as_deref(),
            list_secrets::Variables {
                filter: filter.clone(),
                after: after.clone(),
                first: Some(PAGE_SIZE),
            },
        )
        .await
        .context("failed to list secrets")?;

        rows.extend(response.secrets.edges.into_iter().map(|edge| edge.node));

        let page = response.secrets.page_info;
        if !page.has_next_page || page.end_cursor.is_none() {
            break;
        }
        after = page.end_cursor;
    }

    ctx.write_all(rows, ())
}

async fn do_delete(args: &DeleteArgs, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let variables = if args.recursive {
        delete_secret::Variables {
            catalog_name: None,
            prefix: Some(models::Prefix::new(&args.name)),
        }
    } else {
        delete_secret::Variables {
            catalog_name: Some(models::Name::new(&args.name)),
            prefix: None,
        }
    };

    let response =
        post_graphql::<DeleteSecret>(&ctx.rest, ctx.access_token().as_deref(), variables)
            .await
            .context("failed to delete secret")?;

    ctx.write_all(response.delete_secret.into_iter().map(DeletedSecret), ())
}

/// One name of a `deleteSecret` result, rendered as a row.
#[derive(serde::Serialize)]
struct DeletedSecret(models::Name);

impl output::CliOutput for DeletedSecret {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["Deleted"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        vec![JsonCell(Some(serde_json::Value::String(
            self.0.to_string(),
        )))]
    }
}

/// Read a secret value from `path`, or from stdin. Never from an argument:
/// arguments land in shell history and in process listings.
fn read_input(path: Option<&std::path::Path>) -> anyhow::Result<String> {
    match path {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display())),
        None => {
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .context("failed to read the secret value from stdin")?;
            Ok(input)
        }
    }
}

impl output::CliOutput for list_secrets::SelectSecret {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["Name", "Secret ID"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(self, &["/catalogName", "/secretId"])
    }
}

impl output::CliOutput for set_secret::SetSecretSetSecret {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["Name", "Secret ID", "Changed"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &["/secret/catalogName", "/secret/secretId", "/changed"],
        )
    }
}
