mod roles;

use anyhow::Context;

use crate::controlplane;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Auth {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Authenticate with Flow
    ///
    /// This is typically the first command you'll run with `flowctl`.
    /// Opens your web browser to the /admin/api page and waits for you
    /// to paste the authentication token you get from there.
    /// If you're running in an environment that doesn't have a browser,
    /// then you can alternatively navigate yourself to:
    /// https://dashboard.estuary.dev/admin/api
    /// and then run `flowctl auth token --token <paste-token-here>`
    /// in order to authenticate.
    Login,
    /// Authenticate to Flow using a secret access token.
    ///
    /// You can find this token within Flow UI dashboard under "Admin"
    /// (https://dashboard.estuary.dev/admin/api).
    Token(Token),
    /// Work with authorization roles and grants.
    ///
    /// Roles are prefixes of the Flow catalog namespace.
    /// Granting a role (the object) to another role or user (the subject)
    /// gives the subject a capability (read, write, or admin) to the object role.
    ///
    /// A 'read' capability allows a subject user or specifications to read
    /// from object collections.
    ///
    /// A 'write' capability allows reads and writes to object collections
    /// from the subject.
    ///
    /// An 'admin' capability allows a subject to manage catalog specifications,
    /// grants, and storage mappings which are prefixed by the object role.
    /// Unlike 'read' or 'write', the subject of an 'admin' grant also inherits
    /// capabilities granted to the object role from still-other roles.
    Roles(roles::Roles),

    /// Fetches and prints an auth token that can be used to access a Flow data plane.
    ///
    /// The returned token can be used to access the Flow data plane with 3rd party tools.
    /// For example, you can use curl to access a private port of a running task by running:
    /// ```ignore
    /// curl -H "Authorization: Bearer $(flowctl auth data-plane-access-token --prefix myTenant/)" https://myPort.myHost.data-plane.example/
    /// ```
    DataPlaneAccessToken(DataPlaneAccessToken),
}

#[derive(Debug, clap::Args)]
pub struct DataPlaneAccessToken {
    #[clap(long, required = true)]
    prefix: Vec<String>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Token {
    #[clap(long)]
    token: String,
}

impl Auth {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Login => do_login(ctx).await,
            Command::Token(Token { token }) => {
                controlplane::configure_new_access_token(ctx, token.clone()).await?;
                println!("Configured access token.");
                Ok(())
            }
            Command::Roles(roles) => roles.run(ctx).await,
            Command::DataPlaneAccessToken(args) => do_data_plane_access_token(ctx, args).await,
        }
    }
}

async fn do_login(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    use crossterm::tty::IsTty;

    let url = ctx.config().get_dashboard_url("/admin/api")?.to_string();

    println!("\nopening browser to: {url}");
    open::that(&url).context("failed to open web browser")?;

    if std::io::stdin().is_tty() {
        println!("please paste the token from the CLI auth page and hit Enter");
        let token = tokio::task::spawn_blocking(|| rpassword::prompt_password("Auth Token: "))
            .await?
            .context("failed to read auth token")?;
        // copied credentials will often accidentally contain extra whitespace characters
        let token = token.trim().to_string();
        controlplane::configure_new_access_token(ctx, token).await?;
        println!("\nConfigured access token.");
        Ok(())
    } else {
        // This is not necessarily a problem for the user, because they can just run
        // `auth token --token ...`, but we still need to exit non-zero
        anyhow::bail!(
            "unable to read auth token because flowctl \
            is not running interactively. You can still login non-interactively \
            by running `flowctl auth token --token <paste-token-here>`"
        );
    }
}

async fn do_data_plane_access_token(
    ctx: &mut crate::CliContext,
    args: &DataPlaneAccessToken,
) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    let access =
        crate::dataplane::fetch_data_plane_access_token(client, args.prefix.clone()).await?;
    println!("{}", access.auth_token);
    Ok(())
}
