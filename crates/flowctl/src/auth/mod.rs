use crate::controlplane;
use anyhow::Context;

mod roles;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Auth {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Authenticate to Flow
    ///
    /// Opens a web browser to the CLI login page and waits to read the auth token
    /// from stdin.
    Login,
    /// Authenticate to Flow using a token.
    ///
    /// You can find this token within Flow UI dashboard under "Admin".
    Token(TokenArgs),
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
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct TokenArgs {
    /// Your user token, which can be obtained from https://go.estuary.dev/2DgrAp
    #[clap(long)]
    token: String,
}

impl Auth {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Login => do_login(ctx).await,
            Command::Token(TokenArgs { token }) => {
                controlplane::configure_new_credential(ctx, token).await?;
                println!("Configured credentials.");
                Ok(())
            }
            Command::Roles(roles) => roles.run(ctx).await,
        }
    }
}

async fn do_login(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    let url = ctx.config().dashboard_url("/cli-auth/login")?.to_string();

    if let Err(err) = open::that(&url) {
        tracing::error!(error = %err, url = %url, "failed to open url");
        anyhow::bail!(
            "failed to open browser. Please navigate to {} in order to complete the login process",
            url
        );
    }

    println!("\nOpened web browser to: {}", url);

    let credential = try_read_credential().await.context("unable to read credential from stdin. \
            If you have the credential, you may run `flowctl auth token --token <paste-credential-here>` \
            in order to complete the login process.")?;
    tracing::debug!(credential = %credential, "successfully read credential");
    controlplane::configure_new_credential(ctx, credential.trim()).await?;
    println!("Successfully authenticated. Flowctl is ready to go!");
    Ok(())
}

async fn try_read_credential() -> anyhow::Result<String> {
    use crossterm::tty::IsTty;

    if !std::io::stdin().is_tty() {
        anyhow::bail!("stdin is not a TTY");
    }

    println!("Please login via the browser tab that was just opened.");
    println!(
        "Once you have logged in, paste the credential here and hit Enter to complete the process."
    );
    println!("Waiting on credential...");
    let handle: tokio::task::JoinHandle<std::io::Result<String>> =
        tokio::task::spawn_blocking(|| {
            let mut s = String::with_capacity(512);
            std::io::stdin().read_line(&mut s)?;
            Ok(s)
        });
    let line = handle
        .await?
        .context("failed to read credential from stdin")?;

    if line.trim().is_empty() {
        anyhow::bail!("credential was empty");
    }
    Ok(line)
}
