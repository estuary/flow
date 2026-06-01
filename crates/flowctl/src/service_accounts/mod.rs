use crate::graphql::*;
use crate::output::{self, JsonCell, OutputType, to_table_row};
use anyhow::Context;

/// Page size used when listing service accounts. The server paginates the
/// `serviceAccounts` connection, so `list` walks all pages.
const PAGE_SIZE: i64 = 100;

#[derive(Debug, clap::Args)]
pub struct ServiceAccounts {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// List service accounts you administer.
    List(List),
    /// Create a new service account scoped to a catalog prefix.
    Create(Create),
    /// Disable a service account, revoking its grants and all API keys.
    Disable(Disable),
    /// Re-enable a previously disabled service account.
    ///
    /// Does NOT restore revoked API keys — mint new ones with
    /// `service-accounts api-keys create`.
    Enable(Enable),
    /// Manage a service account's API keys.
    ApiKeys(ApiKeys),
}

#[derive(Debug, clap::Args)]
pub struct List {
    /// Only show service accounts under this catalog prefix.
    #[clap(long)]
    pub prefix: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct Create {
    /// Catalog prefix the service account will be granted access to.
    #[clap(long)]
    pub prefix: String,
    /// Capability to grant on the prefix.
    #[clap(long, value_enum)]
    pub capability: CapabilityArg,
    /// Human-readable display name (e.g. "CI deploy bot").
    #[clap(long)]
    pub name: String,
}

#[derive(Debug, clap::Args)]
pub struct Disable {
    /// The service account's id.
    pub id: uuid::Uuid,
}

#[derive(Debug, clap::Args)]
pub struct Enable {
    /// The service account's id.
    pub id: uuid::Uuid,
}

#[derive(Debug, clap::Args)]
pub struct ApiKeys {
    #[clap(subcommand)]
    cmd: ApiKeysCommand,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum ApiKeysCommand {
    /// List the API keys of a service account.
    List(ApiKeysList),
    /// Create an API key for a service account.
    ///
    /// The secret is printed once to stdout and cannot be retrieved again.
    Create(ApiKeysCreate),
    /// Revoke (delete) an API key.
    Revoke(ApiKeysRevoke),
}

#[derive(Debug, clap::Args)]
pub struct ApiKeysList {
    /// The service account's id.
    pub service_account_id: uuid::Uuid,
}

#[derive(Debug, clap::Args)]
pub struct ApiKeysCreate {
    /// The service account's id.
    pub service_account_id: uuid::Uuid,
    /// Human-readable label (e.g. "github-actions").
    #[clap(long)]
    pub label: String,
    /// ISO 8601 duration the key is valid for (e.g. P90D, P1Y).
    #[clap(long)]
    pub valid_for: String,
}

#[derive(Debug, clap::Args)]
pub struct ApiKeysRevoke {
    /// The API key's id.
    pub id: models::Id,
}

/// Mirrors the read/write/admin grant capabilities. `models::Capability` also
/// has a `None` variant that isn't meaningful when creating a service account,
/// so we expose a CLI-specific enum and convert.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum CapabilityArg {
    Read,
    Write,
    Admin,
}

impl From<CapabilityArg> for models::Capability {
    fn from(arg: CapabilityArg) -> Self {
        match arg {
            CapabilityArg::Read => models::Capability::Read,
            CapabilityArg::Write => models::Capability::Write,
            CapabilityArg::Admin => models::Capability::Admin,
        }
    }
}

impl ServiceAccounts {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::List(args) => do_list(args, ctx).await,
            Command::Create(args) => do_create(args, ctx).await,
            Command::Disable(args) => do_disable(args, ctx).await,
            Command::Enable(args) => do_enable(args, ctx).await,
            Command::ApiKeys(api_keys) => api_keys.run(ctx).await,
        }
    }
}

impl ApiKeys {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            ApiKeysCommand::List(args) => do_api_keys_list(args, ctx).await,
            ApiKeysCommand::Create(args) => do_api_keys_create(args, ctx).await,
            ApiKeysCommand::Revoke(args) => do_api_keys_revoke(args, ctx).await,
        }
    }
}

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/list-query.graphql",
    response_derives = "Serialize,Debug",
    variables_derives = "Clone",
    extern_enums("Capability")
)]
struct ListServiceAccounts;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/create-mutation.graphql",
    response_derives = "Serialize,Debug",
    extern_enums("Capability")
)]
struct CreateServiceAccount;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/disable-mutation.graphql",
    response_derives = "Serialize,Debug"
)]
struct DisableServiceAccount;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/enable-mutation.graphql",
    response_derives = "Serialize,Debug"
)]
struct EnableServiceAccount;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/create-api-key-mutation.graphql",
    response_derives = "Serialize,Debug"
)]
struct CreateApiKey;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/service_accounts/revoke-api-key-mutation.graphql",
    response_derives = "Serialize,Debug"
)]
struct RevokeApiKey;

/// Walks every page of the `serviceAccounts` connection, optionally filtered to
/// a single catalog prefix. Without a filter the server scopes results to the
/// caller's admin prefixes.
async fn fetch_all_service_accounts(
    ctx: &mut crate::CliContext,
    prefix: Option<String>,
) -> anyhow::Result<Vec<list_service_accounts::SelectServiceAccount>> {
    let mut accounts = Vec::new();
    let mut after: Option<String> = None;
    loop {
        // Rebuilt each page rather than cloned, so we don't depend on the
        // generated filter input types deriving Clone.
        let filter = prefix
            .as_ref()
            .map(|p| list_service_accounts::ServiceAccountsFilter {
                catalog_prefix: Some(list_service_accounts::PrefixFilter {
                    starts_with: Some(p.clone()),
                }),
            });
        let vars = list_service_accounts::Variables {
            filter,
            after: after.clone(),
            first: Some(PAGE_SIZE),
        };
        let conn = post_graphql::<ListServiceAccounts>(&ctx.client, vars)
            .await
            .context("failed to list service accounts")?
            .service_accounts;

        accounts.extend(conn.edges.into_iter().map(|edge| edge.node));

        // Continue only while there's both a next page and a cursor to resume from.
        match conn.page_info.end_cursor {
            Some(cursor) if conn.page_info.has_next_page => after = Some(cursor),
            _ => break,
        }
    }
    Ok(accounts)
}

async fn do_list(args: &List, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let accounts = fetch_all_service_accounts(ctx, args.prefix.clone()).await?;
    let rows = accounts
        .into_iter()
        .map(|sa| Ok(ServiceAccountRow(serde_json::to_value(sa)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ctx.write_all(rows, ())
}

async fn do_create(args: &Create, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let vars = create_service_account::Variables {
        prefix: models::Prefix::new(&args.prefix),
        capability: args.capability.into(),
        display_name: args.name.clone(),
    };
    let resp = post_graphql::<CreateServiceAccount>(&ctx.client, vars)
        .await
        .context("failed to create service account")?;

    let row = ServiceAccountRow(serde_json::to_value(resp.create_service_account)?);
    ctx.write_all(std::iter::once(row), ())
}

async fn do_disable(args: &Disable, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let vars = disable_service_account::Variables { id: args.id };
    let resp = post_graphql::<DisableServiceAccount>(&ctx.client, vars)
        .await
        .context("failed to disable service account")?;
    anyhow::ensure!(
        resp.disable_service_account,
        "service account was not disabled"
    );
    println!("Disabled service account {}", args.id);
    Ok(())
}

async fn do_enable(args: &Enable, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let vars = enable_service_account::Variables { id: args.id };
    let resp = post_graphql::<EnableServiceAccount>(&ctx.client, vars)
        .await
        .context("failed to enable service account")?;
    anyhow::ensure!(resp.enable_service_account, "service account was not enabled");
    println!(
        "Enabled service account {}. Previously revoked API keys are not restored; \
         create new ones with `flowctl service-accounts api-keys create`.",
        args.id
    );
    Ok(())
}

async fn do_api_keys_list(args: &ApiKeysList, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    // API keys are only exposed nested under their service account, so locate
    // the account among those the caller administers, then list its keys.
    let account = fetch_all_service_accounts(ctx, None)
        .await?
        .into_iter()
        .find(|sa| sa.id == args.service_account_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no service account {} found among those you administer",
                args.service_account_id
            )
        })?;

    let rows = account
        .api_keys
        .into_iter()
        .map(|key| Ok(ApiKeyRow(serde_json::to_value(key)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ctx.write_all(rows, ())
}

async fn do_api_keys_create(
    args: &ApiKeysCreate,
    ctx: &mut crate::CliContext,
) -> anyhow::Result<()> {
    let vars = create_api_key::Variables {
        service_account_id: args.service_account_id,
        label: args.label.clone(),
        valid_for: args.valid_for.clone(),
    };
    let key = post_graphql::<CreateApiKey>(&ctx.client, vars)
        .await
        .context("failed to create API key")?
        .create_api_key;

    // The secret is shown exactly once. Put human/metadata on stderr so that in
    // the default mode stdout carries only the secret, for clean capture by CI:
    //   SECRET=$(flowctl service-accounts api-keys create $ID --label ci --valid-for P90D)
    eprintln!(
        "Created API key {} for service account {}. \
         Store the secret now — it cannot be retrieved again.",
        key.id, args.service_account_id
    );

    match ctx.get_output_type() {
        OutputType::Table => println!("{}", key.secret),
        OutputType::Json => {
            output::print_json(std::iter::once(ApiKeyCreateRow(serde_json::to_value(&key)?)))?
        }
        OutputType::Yaml => {
            output::print_yaml(std::iter::once(ApiKeyCreateRow(serde_json::to_value(&key)?)))?
        }
    }
    Ok(())
}

async fn do_api_keys_revoke(
    args: &ApiKeysRevoke,
    ctx: &mut crate::CliContext,
) -> anyhow::Result<()> {
    let vars = revoke_api_key::Variables { id: args.id };
    let resp = post_graphql::<RevokeApiKey>(&ctx.client, vars)
        .await
        .context("failed to revoke API key")?;
    anyhow::ensure!(resp.revoke_api_key, "API key was not revoked");
    println!("Revoked API key {}", args.id);
    Ok(())
}

/// Output wrappers around the raw GraphQL JSON. Wrapping `serde_json::Value`
/// (rather than the `graphql_client`-generated structs) keeps the table columns
/// decoupled from the generated type names, and `-o json`/`-o yaml` emit the
/// record verbatim via the transparent representation.
#[derive(serde::Serialize)]
#[serde(transparent)]
struct ServiceAccountRow(serde_json::Value);

impl output::CliOutput for ServiceAccountRow {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec![
            "ID",
            "Name",
            "Prefix",
            "Capability",
            "Created",
            "Last Used",
            "Disabled",
        ]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &[
                "/id",
                "/displayName",
                "/prefix",
                "/capability",
                "/createdAt",
                "/lastUsedAt",
                "/disabledAt",
            ],
        )
    }
}

#[derive(serde::Serialize)]
#[serde(transparent)]
struct ApiKeyRow(serde_json::Value);

impl output::CliOutput for ApiKeyRow {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["ID", "Label", "Created", "Expires", "Last Used"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(
            self,
            &["/id", "/label", "/createdAt", "/expiresAt", "/lastUsedAt"],
        )
    }
}

#[derive(serde::Serialize)]
#[serde(transparent)]
struct ApiKeyCreateRow(serde_json::Value);

impl output::CliOutput for ApiKeyCreateRow {
    type TableAlt = ();
    type CellValue = JsonCell;

    fn table_headers(_: Self::TableAlt) -> Vec<&'static str> {
        vec!["ID", "Secret"]
    }

    fn into_table_row(self, _: Self::TableAlt) -> Vec<Self::CellValue> {
        to_table_row(self, &["/id", "/secret"])
    }
}
