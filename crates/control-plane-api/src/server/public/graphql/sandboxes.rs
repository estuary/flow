//! Sandboxes: per-user Linux VMs that run shell commands, backed by Fly.io
//! Sprites. See `crate::sprites` for the API client and `crate::sandboxes` for
//! the records and lifecycle these operations drive.

use async_graphql::Context;
use std::sync::Arc;

/// A user's sandbox: a persistent Linux VM that runs their shell commands.
///
/// The provider's own name for the VM stays internal: clients address a
/// sandbox by its `catalogName`, scoped to the authenticated user. Creation
/// is authorized on that catalog name.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Sandbox {
    /// Catalog name used to address this sandbox in queries and mutations.
    pub catalog_name: models::Name,
    /// When the sandbox was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Whether flowctl installation and baseline checkpoint creation completed.
    pub ready: bool,
}

#[async_graphql::ComplexObject]
impl Sandbox {
    /// Commands run in this sandbox, newest first. Each is the record of a
    /// command that started, with its observed exit result. A reset or delete
    /// discards them along with their output.
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

/// A command run in a sandbox: what ran, when it was requested, and where the
/// sandbox holds its output. Read output with `sandboxFileRead` and poll
/// `sandbox.execs` for the exit result.
#[derive(Debug, async_graphql::SimpleObject)]
pub struct ExecEvent {
    /// Identifier of the exec, passed with `catalogName` to `sandboxExecCancel`.
    pub exec_id: models::Id,
    /// The bash command that ran.
    pub command: String,
    /// When the command was requested. It started shortly after.
    pub requested_at: chrono::DateTime<chrono::Utc>,
    /// Path of the file the command's stdout is written to, to pass as `path`
    /// to `sandboxFileRead`.
    pub stdout_path: String,
    /// Path of the file the command's stderr is written to, to pass as `path`
    /// to `sandboxFileRead`.
    pub stderr_path: String,
    /// Recorded exit code, or null if no status was recorded. Null can mean
    /// still running, cancelled, or a wrapper failure. Zero means success.
    /// The initial `sandboxExec` response returns null without waiting for exit.
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

/// A read of a sandbox file.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct FileRead {
    #[graphql(skip)]
    pub data: Vec<u8>,
    /// Byte offset into the file after this chunk. Pass it as `offset` on the next
    /// read. It is the requested offset when the file does not exist.
    pub offset: i32,
    /// Whether the file was there. A file the sandbox writes later, such as a
    /// running command's output, reports `false` until it appears, so this
    /// is an answer rather than a failure.
    pub exists: bool,
}

#[async_graphql::ComplexObject]
impl FileRead {
    /// Raw file bytes encoded as standard padded base64. Empty when the file
    /// does not exist. Offset and limit count raw bytes, not encoded characters.
    async fn base64(&self) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &self.data)
    }

    /// File bytes decoded as UTF-8, replacing invalid sequences with U+FFFD.
    /// Byte offsets and limits may split a multi-byte character, so replacements
    /// can appear at chunk boundaries even for valid UTF-8 files. For lossless
    /// text across reads, use `base64` with a streaming UTF-8 decoder.
    /// Empty when the file does not exist.
    async fn utf8(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

#[derive(Debug, Default)]
pub struct SandboxesQuery;

#[async_graphql::Object]
impl SandboxesQuery {
    /// Look up one of the authenticated user's sandboxes by `catalogName`.
    ///
    /// A sandbox that is not theirs, and one since deleted, are both null: the
    /// answer says nothing about whether the catalog name names anything at all.
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

    /// List the authenticated user's sandboxes, newest first.
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

    /// Read `path` in the authenticated user's sandbox `catalogName`, resolved
    /// against the sandbox user's home directory, from byte `offset` onwards.
    /// `path` must be relative and must not contain `..` components. A read
    /// returns at most `limit` bytes, and never more than 1 MiB, so continue a
    /// longer file from the `offset` you are handed.
    /// Offset and limit count raw file bytes for both `base64` and `utf8`, before
    /// base64 encoding or UTF-8 decoding.
    ///
    /// A path that does not exist returns `exists: false` rather than an
    /// error, because a sandbox writes files while a client watches for them.
    ///
    /// This reads a command's result too. An `ExecEvent` carries
    /// `stdoutPath` and `stderrPath`; read them while the command runs or
    /// afterwards, and poll `sandbox.execs` for a non-null `exitCode`.
    /// The wrapper records that result after the command's last output, so once
    /// you see it, read output through EOF from your current offset to get the
    /// remainder.
    ///
    /// A sandbox reset discards past commands, taking their output files with
    /// them, after which those paths no longer exist.
    async fn sandbox_file_read(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        path: String,
        #[graphql(desc = "Byte offset to read from", default = 0)] offset: i32,
        #[graphql(
            desc = "Most bytes to return. Omitted, or larger than the 1 MiB ceiling, reads up to that ceiling."
        )]
        limit: Option<i32>,
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
            .map_err(|err| match err {
                crate::sandboxes::FileReadError::Other(err) => {
                    tracing::error!(?err, %sandbox.id, %path, "failed to read sandbox file");
                    async_graphql::Error::new(format!("failed to read sandbox file: {err:#}"))
                }
                refused => async_graphql::Error::new(refused.to_string()),
            })?;

        Ok(FileRead {
            data: chunk.bytes,
            // GraphQL `Int` is 32 bits, so a file beyond two gigabytes saturates.
            offset: i32::try_from(chunk.offset).unwrap_or(i32::MAX),
            exists: chunk.exists,
        })
    }
}

#[derive(Debug, Default)]
pub struct SandboxesMutation;

#[async_graphql::Object]
impl SandboxesMutation {
    /// Create a sandbox for the authenticated user at `catalogName`.
    /// Requires CreateSandbox on that catalog name, which must differ from all
    /// other live sandboxes. A deleted sandbox frees its catalog name.
    ///
    /// When this returns the sandbox is ready to use: it accepts commands,
    /// flowctl is installed, and the baseline that `sandboxReset` restores
    /// exists.
    async fn sandbox_create(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Sandbox> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        validator::Validate::validate(&catalog_name)
            .map_err(|err| async_graphql::Error::new(format!("invalid catalog name: {err}")))?;
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateSandbox,
        )
        .await?;
        let client = sprites_client(ctx)?;

        // A refused name is the caller's to fix and says so in its
        // message; only a failure of the control plane or provider is logged.
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

    /// Run a bash command in the authenticated user's sandbox `catalogName`.
    /// This returns once the command is running, without waiting for it to
    /// finish, and the command then runs for as long as it takes. The returned
    /// `ExecEvent` carries the paths its result lands at: read `stdoutPath`
    /// and `stderrPath` with `sandboxFileRead` while the command runs or
    /// afterwards, and poll `sandbox.execs` for `exitCode`. A command that
    /// fails to start is not recorded, and this returns the failure instead.
    ///
    /// The command is passed to `bash -lc`, so it may use shell syntax such as
    /// pipes and redirection. The server generates a new `execId` for each call.
    async fn sandbox_exec(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        command: String,
        #[graphql(
            desc = "Complete UTF-8 stdin (at most 1 MiB), followed by EOF. Omitted or empty input gives immediate EOF. Sent in the request body, not stored in the exec record."
        )]
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

        // The command may carry credentials, so it is recorded in
        // exec metadata and logged nowhere.
        let event = crate::sandboxes::exec(&client, exec_id, &sandbox, &command, stdin.as_deref())
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to start sandbox command");
                async_graphql::Error::new(format!("failed to start command: {err:#}"))
            })?;

        tracing::info!(%sandbox.id, %event.id, "started sandbox command");
        Ok(event.into())
    }

    /// Stop the native exec session for the authenticated user's `execId`,
    /// including descendants in its process group. Returns false if an exit
    /// status was already recorded or the session no longer exists.
    ///
    /// The output it wrote before it stopped stays readable at its
    /// `stdoutPath` and `stderrPath`. The final exit code returned by Fly is
    /// recorded after termination; if recording fails, this returns an error
    /// and the exit code may remain null.
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

    /// Reset the authenticated user's sandbox `catalogName` to its provisioning
    /// baseline, discarding every change made since. Past commands and their
    /// output are discarded too, so their exec ids stop resolving. The sandbox
    /// keeps its catalog name.
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

    /// Delete the authenticated user's sandbox `catalogName`, including its
    /// filesystem and the record of commands run in it. Its catalog name becomes
    /// available for reuse. Unready records can be deleted even if provider
    /// cleanup fails.
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

/// Fetches sandbox `catalog_name` if it is the caller's. A sandbox that is not theirs,
/// and one that no longer exists, are both absent, so an answer built from
/// this cannot tell them apart.
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

/// Resolves sandbox `catalog_name` for an operation that must act on one, turning the
/// absence [`fetch_sandbox`] reports into the error the caller sees.
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
