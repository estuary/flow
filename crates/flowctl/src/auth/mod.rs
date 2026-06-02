pub mod list;
mod roles;

use anyhow::Context;

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
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Token {
    #[clap(long)]
    token: String,
}

impl Auth {
    /// Whether this subcommand acquires user credentials (`login` / `token`).
    /// These are the only contexts permitted to create a new refresh token, and
    /// they reject an ambient `FLOW_AUTH_TOKEN`.
    pub fn acquires_credential(&self) -> bool {
        matches!(self.cmd, Command::Login | Command::Token(_))
    }

    /// Acquire the access token of a credential-establishing subcommand
    /// (`login`, `token`) to seed the live token watch, which in turn drives
    /// token creation, refresh, and persistence.
    ///
    /// The returned token deliberately clears a prior session refresh token,
    /// which may belong to a different identity than the freshly-provided access
    /// token, or be expired or revoked. Omitting it forces the watch down its
    /// create-refresh-token path, minting a matching refresh token and round-
    /// tripping to the server, so an invalid access token is rejected rather
    /// than silently persisted.
    ///
    /// Panics if the subcommand does not acquire a credential; callers gate this
    /// behind `acquires_credential()`.
    pub async fn acquire_credential(
        &self,
        config: &crate::config::Config,
    ) -> anyhow::Result<flow_client_next::user_auth::UserToken> {
        let access_token = match &self.cmd {
            Command::Login => do_login(config).await?,
            Command::Token(Token { token }) => token.clone(),
            Command::Roles(_) => unreachable!("gated by acquires_credential()"),
        };
        Ok(flow_client_next::user_auth::UserToken {
            access_token: Some(access_token),
            refresh_token: None,
        })
    }

    /// Run consume-only auth subcommands (`roles`), and confirm credential
    /// acquisition for `login`/`token` — whose access token is acquired via
    /// `acquire_credential` to seed the watch before it's wired.
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::Roles(roles) => roles.run(ctx).await,
            Command::Login | Command::Token(_) => {
                if let Err(status) = ctx.user_tokens.watch().token().result() {
                    anyhow::bail!(
                        "Failed to complete authentication flow: {}",
                        status.message()
                    )
                } else {
                    let claims = ctx.require_control_claims()?;
                    match claims.email {
                        Some(email) => println!("Authenticated as {email}."),
                        None => println!("Authenticated as User ID {}", claims.sub),
                    }
                    Ok(())
                }
            }
        }
    }
}

async fn do_login(config: &crate::config::Config) -> anyhow::Result<String> {
    use crossterm::tty::IsTty;

    let url = config.get_dashboard_url().join("/admin/api")?.to_string();

    println!("\nOpening browser to: {url}");
    if let Err(_) = open::that(&url) {
        println!(
            "... Failed to open your browser. You can continue by manually clicking the link."
        );
    };

    if std::io::stdin().is_tty() {
        let token = tokio::task::spawn_blocking::<_, anyhow::Result<String>>(|| {
            // Use MemHistory because the default is file-backed, and we don't need it causing problems.
            // History is not used at all, so this is just a bit of extra caution.
            let mut rl: rustyline::Editor<(), rustyline::history::MemHistory> =
                rustyline::Editor::with_history(
                    rustyline::Config::default(),
                    rustyline::history::MemHistory::new(),
                )?;

            let line = rl.readline(
                "please paste the token from the CLI auth page and hit Enter\nAuth token: ",
            )?;
            Ok(line)
        })
        .await?
        .context("failed to read auth token")?;

        // Copied credentials will often accidentally contain extra whitespace characters.
        Ok(token.trim().to_string())
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
