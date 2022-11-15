use serde::Deserialize;

use super::config;

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
    /// Authenticate to Flow using a service account file.
    ///
    /// You can find this service account within Flow UI dashboard under "Admin".
    ServiceAccount(ServiceAccountArgs),
    /// Authenticate to a local development instance of the Flow control plane.
    ///
    /// This is intended for developers who are running local instances
    /// of the Flow control and data-planes.
    Develop(Develop),
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
pub struct ServiceAccountArgs {
    /// Path to service account JSON file. Use `-` to read from stdin.
    #[clap(value_parser)]
    file: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceAccount {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Develop {
    #[clap(long)]
    token: Option<String>,
}

impl Auth {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::ServiceAccount(ServiceAccountArgs { file }) => {
                if let Some(file) = file {
                    let (sa, msg): (ServiceAccount, &str) = if file == "-" {
                        (serde_json::from_reader(std::io::stdin())?, "Configured service account.")
                    } else {
                        let sa_file = std::fs::File::open(file)?;
                        (serde_json::from_reader(sa_file)?, "Configured service account. You may now delete the file.")
                    };
                    ctx.config_mut().api = Some(config::API::managed(sa));
                    println!("{}", msg);
                } else {
                    println!("You can get your service-account from https://dashboard.estuary.dev/admin/api")
                }
                Ok(())
            }
            Command::Develop(Develop { token }) => {
                ctx.config_mut().api = Some(config::API::development(token.clone()));
                println!("Configured for local development.");
                Ok(())
            }
            Command::Roles(roles) => roles.run(ctx).await
        }
    }
}
