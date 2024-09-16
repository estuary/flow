use crate::api_exec_paginated;
use crate::output::CliOutput;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Roles {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// List all user and role grants.
    ///
    /// The listing includes all user-grants and role-grants which
    /// are visible to you. A grant is visible if you are the grant
    /// recipient, or if you are an administrator of the object role.
    List,
    /// Grant an object role to a subject user or role.
    ///
    /// A grant bestows a capability to the --subject-user-id or --subject-role
    /// to act upon the object role in accordance with the granted capability.
    /// If a grant already exists, it is updated.
    ///
    /// One of either --subject-user-id or --subject-role must be provided.
    Grant(Grant),
    /// Revoke an object role from a subject user or role.
    ///
    /// One of either --subject-user-id or --subject-role must be provided.
    Revoke(Revoke),
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
enum Capability {
    Read,
    Write,
    Admin,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Grant {
    /// Subject user Id (a UUID) to which the grant should be issued.
    #[clap(
        long,
        conflicts_with("subject-role"),
        required_unless_present("subject-role")
    )]
    subject_user_id: Option<uuid::Uuid>,
    /// Subject role which receives the grant.
    #[clap(long, conflicts_with("subject-user-id"))]
    subject_role: Option<String>,
    /// Object role to which the subject role is granted.
    #[clap(long)]
    object_role: String,
    /// Granted capability of the subject to the object role.
    #[clap(long, value_enum)]
    capability: Capability,
    /// Free-form details of the grant, such as a reason or audit log message.
    #[clap(long)]
    detail: Option<String>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Revoke {
    /// Subject user Id (a UUID) from which the grant is revoked.
    #[clap(
        long,
        conflicts_with("subject-role"),
        required_unless_present("subject-role")
    )]
    subject_user_id: Option<uuid::Uuid>,
    /// Subject role from which the grant is revoked.
    #[clap(long, conflicts_with("subject-user-id"))]
    subject_role: Option<String>,
    /// Object role from which the subject role is revoked.
    #[clap(long)]
    object_role: String,
}

impl Roles {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> Result<(), anyhow::Error> {
        match &self.cmd {
            Command::Grant(grant) => do_grant(ctx, grant).await,
            Command::List => do_list(ctx).await,
            Command::Revoke(revoke) => do_revoke(ctx, revoke).await,
        }
    }
}

pub async fn do_list(ctx: &mut crate::CliContext) -> anyhow::Result<()> {
    #[derive(Deserialize, Serialize)]
    struct Row {
        capability: String,
        created_at: crate::Timestamp,
        detail: Option<String>,
        object_role: String,
        subject_role: Option<String>,
        updated_at: crate::Timestamp,
        user_email: Option<String>,
        user_full_name: Option<String>,
        user_id: Option<uuid::Uuid>,
    }

    impl CliOutput for Row {
        type TableAlt = ();

        type CellValue = String;

        fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
            vec![
                "Subject",
                "Capability",
                "Object",
                "Detail",
                "Created",
                "Updated",
            ]
        }

        fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
            vec![
                match self.subject_role {
                    Some(s) => s,
                    None => crate::format_user(self.user_email, self.user_full_name, self.user_id),
                },
                self.capability,
                self.object_role,
                self.detail.unwrap_or_default(),
                self.created_at.to_string(),
                self.updated_at.to_string(),
            ]
        }
    }
    let rows: Vec<Row> = api_exec_paginated(
        ctx.controlplane_client()
            .await?
            .from("combined_grants_ext")
            .select(
                vec![
                    "capability",
                    "created_at",
                    "detail",
                    "object_role",
                    "subject_role",
                    "updated_at",
                    "user_email",
                    "user_full_name",
                    "user_id",
                ]
                .join(","),
            )
            .order("user_email,subject_role,object_role"),
    )
    .await?;

    ctx.write_all(rows, ())
}

pub async fn do_grant(
    ctx: &mut crate::CliContext,
    Grant {
        subject_user_id,
        subject_role,
        object_role,
        capability,
        detail,
    }: &Grant,
) -> anyhow::Result<()> {
    tracing::debug!(?subject_user_id, ?subject_role, ?object_role, ?capability);

    // Upsert user grants to `user_grants` and role grants to `role_grants`.
    let rows: Vec<GrantRevokeRow> = if let Some(subject_user_id) = subject_user_id {
        api_exec_paginated(
            ctx.controlplane_client()
                .await?
                .from("user_grants")
                .select(grant_revoke_columns())
                .upsert(
                    json!({
                        "user_id": subject_user_id,
                        "object_role": object_role,
                        "capability": capability,
                        "detail": detail,
                    })
                    .to_string(),
                )
                .on_conflict("user_id,object_role"),
        )
        .await?
    } else if let Some(subject_role) = subject_role {
        api_exec_paginated(
            ctx.controlplane_client()
                .await?
                .from("role_grants")
                .select(grant_revoke_columns())
                .upsert(
                    json!({
                        "subject_role": subject_role,
                        "object_role": object_role,
                        "capability": capability,
                        "detail": detail,
                    })
                    .to_string(),
                )
                .on_conflict("subject_role,object_role"),
        )
        .await?
    } else {
        panic!("expected subject role or user ID");
    };

    ctx.write_all(rows, ())
}

pub async fn do_revoke(
    ctx: &mut crate::CliContext,
    Revoke {
        subject_user_id,
        subject_role,
        object_role,
    }: &Revoke,
) -> anyhow::Result<()> {
    tracing::info!(?subject_user_id, ?subject_role, ?object_role);

    // Revoke user grants from `user_grants` and role grants from `role_grants`.
    let rows: Vec<GrantRevokeRow> = if let Some(subject_user_id) = subject_user_id {
        api_exec_paginated(
            ctx.controlplane_client()
                .await?
                .from("user_grants")
                .select(grant_revoke_columns())
                .eq("user_id", subject_user_id.to_string())
                .eq("object_role", object_role)
                .delete(),
        )
        .await?
    } else if let Some(subject_role) = subject_role {
        api_exec_paginated(
            ctx.controlplane_client()
                .await?
                .from("role_grants")
                .select(grant_revoke_columns())
                .eq("subject_role", subject_role)
                .eq("object_role", object_role)
                .delete(),
        )
        .await?
    } else {
        panic!("expected subject role or user ID");
    };

    ctx.write_all(rows, ())
}

#[derive(Deserialize, Serialize, Debug)]
struct GrantRevokeRow {
    capability: String,
    created_at: crate::Timestamp,
    detail: Option<String>,
    object_role: String,
    updated_at: crate::Timestamp,
}

impl CliOutput for GrantRevokeRow {
    type TableAlt = ();
    type CellValue = crate::output::JsonCell;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Capability", "Object", "Detail", "Created", "Updated"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        crate::output::to_table_row(
            self,
            &[
                "/capability",
                "/created_at",
                "/detail",
                "/object_role",
                "/updated_at",
            ],
        )
    }
}

fn grant_revoke_columns() -> String {
    vec![
        "capability",
        "created_at",
        "detail",
        "object_role",
        "updated_at",
    ]
    .join(",")
}
