//! GraphQL operations over [`crate::sandboxes`].

use async_graphql::Context;
use std::sync::Arc;

/// A persistent Linux VM that runs the user's shell commands.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Sandbox {
    pub catalog_name: models::Name,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// False until a new sandbox reaches its baseline state (flowctl installed).
    pub ready: bool,
}

#[async_graphql::ComplexObject]
impl Sandbox {
    /// Commands started in this sandbox
    async fn execs(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<Vec<ExecEvent>>> {
        let env = ctx.data::<crate::Envelope>()?;

        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, self.catalog_name.as_str()).await?;
        let execs = crate::sandboxes::list_execs(&client, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to list execs");
                async_graphql::Error::new("failed to list execs")
            })?;

        Ok(Some(execs.into_iter().map(ExecEvent::from).collect()))
    }
}

impl From<crate::sandboxes::Sandbox> for Sandbox {
    fn from(sandbox: crate::sandboxes::Sandbox) -> Self {
        Self {
            catalog_name: models::Name::new(sandbox.catalog_name),
            created_at: sandbox.created_at,
            ready: sandbox.baseline_checkpoint_id.is_some(),
        }
    }
}

/// A command run in a sandbox.
#[derive(Debug, async_graphql::SimpleObject)]
pub struct ExecEvent {
    pub exec_id: models::Id,
    pub command: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    /// Poll stdout with this `path` and `sandboxFileRead`.
    pub stdout_path: String,
    /// Poll stderr with this `path` and `sandboxFileRead`.
    pub stderr_path: String,
    /// Null until the command exits (crashed execs may not record their exit code)
    pub exit_code: Option<i32>,
}

impl From<crate::sandboxes::ExecEvent> for ExecEvent {
    fn from(event: crate::sandboxes::ExecEvent) -> Self {
        use crate::sandboxes::ExecFile;

        Self {
            exec_id: event.id,
            command: event.command,
            requested_at: event.requested_at,
            stdout_path: ExecFile::Stdout.path(event.id),
            stderr_path: ExecFile::Stderr.path(event.id),
            exit_code: event.exit_code,
        }
    }
}

#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct FileRead {
    #[graphql(skip)]
    pub data: Vec<u8>,
    pub offset: i32,
}

#[async_graphql::ComplexObject]
impl FileRead {
    async fn base64(&self) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &self.data)
    }

    async fn utf8(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

#[derive(Debug, Default)]
pub struct SandboxesQuery;

#[async_graphql::Object]
impl SandboxesQuery {
    async fn sandbox(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Option<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;

        Ok(fetch_sandbox(env, catalog_name.as_str())
            .await?
            .map(Sandbox::from))
    }

    async fn sandboxes(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let sandboxes = crate::sandboxes::list(&env.pg_pool, claims.sub)
            .await
            .map_err(|err| {
                tracing::error!(?err, %claims.sub, "failed to list sandboxes");
                async_graphql::Error::new("failed to list sandboxes")
            })?;

        Ok(sandboxes.into_iter().map(Sandbox::from).collect())
    }

    async fn sandbox_file_read(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        path: String,
        #[graphql(desc = "Byte offset", default = 0)] offset: i32,
        #[graphql(desc = "Max 1 MiB")] limit: Option<i32>,
    ) -> async_graphql::Result<FileRead> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        let offset = u64::try_from(offset)
            .map_err(|_| async_graphql::Error::new("offset must not be negative"))?;
        let limit = limit
            .map(u64::try_from)
            .transpose()
            .map_err(|_| async_graphql::Error::new("limit must not be negative"))?;

        let chunk = crate::sandboxes::read_file(&client, &sandbox, &path, offset, limit)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, %path, "failed to read sandbox file");
                async_graphql::Error::new(format!("failed to read sandbox file: {err:#}"))
            })?;

        Ok(FileRead {
            data: chunk.bytes,
            offset: i32::try_from(chunk.offset).unwrap_or(i32::MAX),
        })
    }
}

#[derive(Debug, Default)]
pub struct SandboxesMutation;

#[async_graphql::Object]
impl SandboxesMutation {
    /// Returns after flowctl is installed.
    async fn sandbox_create(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Sandbox> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateSandbox,
        )
        .await?;
        let client = sprites_client(ctx)?;

        let sandbox =
            crate::sandboxes::create(&client, &env.pg_pool, claims.sub, catalog_name.as_str())
                .await
                .map_err(|err| match err {
                    crate::sandboxes::CreateError::Other(err) => {
                        tracing::error!(?err, %claims.sub, "failed to create sandbox");
                        async_graphql::Error::new(format!("failed to create sandbox: {err:#}"))
                    }
                    refused => async_graphql::Error::new(refused.to_string()),
                })?;

        tracing::info!(%claims.sub, %sandbox.id, "created sandbox");
        Ok(sandbox.into())
    }

    /// Returns once the command has started - read its output by polling `sandboxFileRead`.
    async fn sandbox_exec(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        command: String,
        stdin: Option<String>,
    ) -> async_graphql::Result<ExecEvent> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        let exec_id = ctx
            .data::<Arc<crate::App>>()?
            .id_generator
            .lock()
            .unwrap()
            .next();

        let event = crate::sandboxes::exec(&client, exec_id, &sandbox, &command, stdin.as_deref())
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to start sandbox command");
                async_graphql::Error::new(format!("failed to start command: {err:#}"))
            })?;

        tracing::info!(%sandbox.id, %event.id, "started sandbox command");
        Ok(event.into())
    }

    async fn sandbox_exec_cancel(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        exec_id: models::Id,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        let cancelled = crate::sandboxes::cancel_exec(&client, &sandbox, exec_id)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, %exec_id, "failed to cancel sandbox command");
                async_graphql::Error::new(format!("failed to cancel command: {err:#}"))
            })?;

        tracing::info!(%sandbox.id, %exec_id, cancelled, "cancelled sandbox command");
        Ok(cancelled)
    }

    /// Restore the sandbox to its baseline, discarding all changes, past
    /// commands, and their output.
    async fn sandbox_reset(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        crate::sandboxes::reset(&client, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to reset sandbox");
                async_graphql::Error::new(format!("failed to reset sandbox: {err:#}"))
            })?;

        Ok(true)
    }

    async fn sandbox_delete(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        crate::sandboxes::delete(&client, &env.pg_pool, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to delete sandbox");
                async_graphql::Error::new(format!("failed to delete sandbox: {err:#}"))
            })?;

        Ok(true)
    }
}

async fn fetch_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<Option<crate::sandboxes::Sandbox>> {
    let claims = env.claims()?;

    crate::sandboxes::fetch_by_catalog_name(&env.pg_pool, catalog_name, claims.sub)
        .await
        .map_err(|err| {
            tracing::error!(?err, %catalog_name, %claims.sub, "failed to look up sandbox");
            async_graphql::Error::new("failed to look up sandbox")
        })
}

async fn resolve_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<crate::sandboxes::Sandbox> {
    fetch_sandbox(env, catalog_name)
        .await?
        .ok_or_else(|| async_graphql::Error::new("sandbox not found"))
}

fn sprites_client(ctx: &Context<'_>) -> async_graphql::Result<Arc<crate::sprites::Client>> {
    ctx.data::<Arc<crate::App>>()?
        .sprites
        .clone()
        .ok_or_else(|| async_graphql::Error::new("Sandboxes are not configured"))
}
