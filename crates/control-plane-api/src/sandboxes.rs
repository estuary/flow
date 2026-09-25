//! A sandbox is a Fly.io Sprite (see [`crate::sprites`]) that runs a user's
//! shell commands. Exec metadata, output, and exit status live only in the
//! sandbox under `.estuary/exec/<id>`, so reset and delete discard them.

use anyhow::Context;
use futures::StreamExt;

// TODO: Add a reaper that deletes `sbx-` sprites with no `internal.sandboxes`
// record. Failed creates and deletes leave such sprites behind.
const HANDLE_PREFIX: &str = "sbx-";

const EXEC_DIR: &str = ".estuary/exec";
const SPRITE_HOME: &str = "/home/sprite";

/// This wrapper script separates command execution from the request connection -
/// stdin is written to a file immediately, and command output is redirected to a file,
/// allowing clients to drop the connection and poll for (or potentially stream, in
/// the future) the output on their own terms.
/// This approach also meets the requirement of excluding sensitive user data contained in the command i/o
/// from the control plane database
const EXEC_WRAPPER: &str = r#"
d="$HOME/.estuary/exec/$1"
mkdir -p "$d" || exit 125
exec 2> "$d/wrapper.err"
# write stdin to a file for the command to read after the connection is dropped
cat > "$d/stdin" || exit 125
printf '%s\n' "$3" > "$d/metadata.json" || exit 125
bash -lc "$2" < "$d/stdin" > "$d/stdout" 2> "$d/stderr" &
job=$!
echo started
wait "$job"
status=$?
# Publish the completed status atomically so readers never see an empty file.
echo "$status" > "$d/exit.tmp" && mv "$d/exit.tmp" "$d/exit"
exit "$status"
"#;

/// Prints one `[metadata, exit status or null]` JSON line per exec with
/// metadata. Failed launches can leave directories without it. Rust
/// serializes the metadata, so newlines in a command stay escaped.
const LIST_EXECS: &str = r#"
for d in "$HOME"/.estuary/exec/*; do
    [ -f "$d/metadata.json" ] || continue
    printf '['
    cat "$d/metadata.json" || exit 1
    printf ','
    if [ -f "$d/exit" ]; then cat "$d/exit" || exit 1; else printf 'null'; fi
    printf ']\n'
done
"#;

/// Invoked as `bash -c READ_FILE flow-read-file <path> <offset> <limit>`,
/// so the arguments need no quoting.
const READ_FILE: &str = r#"
cd "$HOME" || exit 1
dd if="$1" iflag=skip_bytes,count_bytes bs=65536 skip="$2" count="$3" status=none
"#;

const FILE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub const READ_MAX_BYTES: u64 = 1024 * 1024;

/// Bounds only the wait for the wrapper's startup announcement, not the command.
const START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct Sandbox {
    pub id: models::Id,
    pub user_id: uuid::Uuid,
    /// The provider's name for the sandbox. It appears in the sprite's
    /// hostname, so it derives from `id` and carries no user identifier.
    pub handle: String,
    /// Catalog name on which creation was authorized.
    pub catalog_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub baseline_checkpoint_id: Option<String>,
}

/// Persisted exec metadata. `exit_code` is not persisted; listings fill it in.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecEvent {
    pub id: models::Id,
    pub command: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecFile {
    Stdout,
    Stderr,
    /// May remain absent after a wrapper failure.
    Exit,
}

impl ExecFile {
    /// Relative to [`SPRITE_HOME`].
    pub fn path(self, exec_id: models::Id) -> String {
        let name = match self {
            ExecFile::Stdout => "stdout",
            ExecFile::Stderr => "stderr",
            ExecFile::Exit => "exit",
        };
        format!("{EXEC_DIR}/{exec_id}/{name}")
    }
}

#[derive(Debug)]
pub struct FileChunk {
    pub bytes: Vec<u8>,
    pub offset: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    /// Catalog names are unique across all users.
    #[error("a sandbox named {0:?} already exists")]
    NameTaken(String),
    #[error("invalid catalog name: {0}")]
    InvalidName(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Returns once the sandbox has flowctl installed and the baseline that
/// [`reset`] restores. An unready record reserves the catalog name during
/// provisioning, so no database connection is held while it runs.
pub async fn create(
    client: &crate::sprites::Client,
    pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
    catalog_name: &str,
) -> Result<Sandbox, CreateError> {
    validator::Validate::validate(&models::Name::new(catalog_name))
        .map_err(|err| CreateError::InvalidName(err.to_string()))?;
    let mut sandbox = {
        let mut conn = pool
            .acquire()
            .await
            .context("acquiring sandbox connection")?;
        persist_record(&mut conn, user_id, catalog_name).await?
    };
    let result = async {
        client.create_sprite(&sandbox.handle).await?;
        let baseline_checkpoint_id = prepare_baseline(client, &sandbox.handle).await?;
        sqlx::query(
            "update internal.sandboxes set baseline_checkpoint_id = $2, updated_at = now() where id = $1",
        )
        .bind(sandbox.id)
        .bind(&baseline_checkpoint_id)
        .execute(pool)
        .await
        .context("marking sandbox ready")?;
        sandbox.baseline_checkpoint_id = Some(baseline_checkpoint_id);
        Ok::<_, anyhow::Error>(())
    }
    .await;

    if let Err(err) = result {
        sqlx::query("delete from internal.sandboxes where id = $1")
            .bind(sandbox.id)
            .execute(pool)
            .await
            .with_context(|| format!("removing failed sandbox creation after: {err:#}"))?;
        return Err(err.into());
    }
    tracing::info!(%sandbox.id, %sandbox.handle, %user_id, "created sandbox");
    Ok(sandbox)
}

/// Fetches sandboxes whose catalog name is one of `names` or starts with one of
/// `prefixes`, newest first. The caller must have authorized access to each of
/// `names` and `prefixes`.
pub async fn fetch(
    pool: &sqlx::PgPool,
    names: &[String],
    prefixes: &[String],
) -> anyhow::Result<Vec<Sandbox>> {
    sqlx::query_as!(
        Sandbox,
        r#"
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at, baseline_checkpoint_id
        from internal.sandboxes
        where catalog_name = any($1) or catalog_name ^@ any($2)
        order by created_at desc
        "#,
        names,
        prefixes,
    )
    .fetch_all(pool)
    .await
    .context("fetching sandbox records")
}

pub async fn list_execs(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
) -> anyhow::Result<Vec<ExecEvent>> {
    let output = exec_output(
        client,
        sandbox,
        &["bash", "-c", LIST_EXECS],
        FILE_READ_TIMEOUT,
    )
    .await
    .context("listing sandbox execs")?;
    anyhow::ensure!(
        output.exit_code == 0,
        "listing sandbox execs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    execs_from_metadata(&output.stdout)
}

fn execs_from_metadata(data: &[u8]) -> anyhow::Result<Vec<ExecEvent>> {
    let mut events = serde_json::Deserializer::from_slice(data)
        .into_iter::<(ExecEvent, Option<i32>)>()
        .map(|result| {
            result.map(|(mut event, exit_code)| {
                event.exit_code = exit_code;
                event
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .context("reading sandbox exec metadata")?;
    events.sort_by(|a, b| {
        b.requested_at
            .cmp(&a.requested_at)
            .then_with(|| b.id.cmp(&a.id))
    });
    Ok(events)
}

async fn persist_record(
    conn: &mut sqlx::PgConnection,
    user_id: uuid::Uuid,
    catalog_name: &str,
) -> Result<Sandbox, CreateError> {
    // The handle derives from the generated id, so both come from one statement.
    // `replace` strips the colons of the macaddr8 text form.
    let sandbox = sqlx::query_as!(
        Sandbox,
        r#"
        insert into internal.sandboxes (id, user_id, handle, catalog_name)
        select id, $1, $2 || replace(id::text, ':', ''), $3
        from (select internal.id_generator() as id) as generated
        returning id as "id!: models::Id", user_id, handle, catalog_name, created_at, baseline_checkpoint_id
        "#,
        user_id,
        HANDLE_PREFIX,
        catalog_name,
    )
    .fetch_one(conn)
    .await
    .map_err(|err| match err.as_database_error() {
        Some(db) if db.constraint() == Some("sandboxes_catalog_name_idx") => {
            CreateError::NameTaken(catalog_name.to_string())
        }
        _ => CreateError::Other(anyhow::Error::new(err).context("creating sandbox record")),
    })?;

    Ok(sandbox)
}

/// Returns once `command` is running, not when it exits. If the connection
/// fails before startup is acknowledged, the command may still be running.
pub async fn exec(
    client: &crate::sprites::Client,
    id: models::Id,
    sandbox: &Sandbox,
    command: &str,
    stdin: Option<&str>,
) -> anyhow::Result<ExecEvent> {
    anyhow::ensure!(
        sandbox.baseline_checkpoint_id.is_some(),
        "sandbox is not ready"
    );
    let event = ExecEvent {
        id,
        command: command.to_owned(),
        requested_at: chrono::Utc::now(),
        exit_code: None,
    };
    let metadata = serde_json::to_string(&event).context("serializing exec metadata")?;
    let exec_id = event.id.to_string();
    let argv = [
        "bash",
        "-c",
        EXEC_WRAPPER,
        "flow-exec",
        &exec_id,
        command,
        &metadata,
    ];

    let session_id = launch(client, sandbox, &argv, stdin).await?;
    // TODO: The command is already running here. If this write fails,
    // the caller gets an error and no exec id, yet the exec is listed
    // and runs to completion, cannot be cancelled without its session
    // ID, and runs again if the caller retries.
    client
        .write_file(
            &sandbox.handle,
            SPRITE_HOME,
            &format!("{EXEC_DIR}/{exec_id}/session_id"),
            session_id,
        )
        .await
        .context("command started but saving its cancellation session ID failed")?;
    Ok(event)
}

/// Returns the session ID once the wrapper announces that the command is
/// running. Any stderr frame means the command did not start: the wrapper
/// redirects stderr to a file right after it creates the exec directory.
async fn launch(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    stdin: Option<&str>,
) -> anyhow::Result<String> {
    let mut frames = client
        .exec_stream(&sandbox.handle, argv, stdin, START_TIMEOUT)
        .await?;

    let mut session_id = None;
    let mut started = false;
    while let Some(frame) = frames.next().await {
        match frame.context("waiting for the command to start")? {
            crate::sprites::Frame::SessionInfo { session_id: id } => session_id = Some(id),
            crate::sprites::Frame::Stdout(_) => started = true,
            crate::sprites::Frame::Stderr(message) => {
                anyhow::bail!(
                    "command failed to start: {}",
                    String::from_utf8_lossy(&message).trim()
                );
            }
            crate::sprites::Frame::Exit(status) => {
                anyhow::bail!(
                    "command wrapper exited with status {status} before startup was confirmed"
                );
            }
        }
        if started && let Some(id) = session_id.take() {
            return Ok(id);
        }
    }
    anyhow::bail!("exec stream ended before startup and session ID were received")
}

/// A relative `path` resolves against [`SPRITE_HOME`].
pub async fn read_file(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    path: &str,
    offset: u64,
    limit: Option<u64>,
) -> anyhow::Result<FileChunk> {
    let offset_arg = offset.to_string();
    let limit_arg = limit
        .unwrap_or(READ_MAX_BYTES)
        .min(READ_MAX_BYTES)
        .to_string();
    let argv = [
        "bash",
        "-c",
        READ_FILE,
        "flow-read-file",
        path,
        &offset_arg,
        &limit_arg,
    ];

    let output = exec_output(client, sandbox, &argv, FILE_READ_TIMEOUT).await?;

    chunk_from_output(offset, output)
}

fn chunk_from_output(offset: u64, output: crate::sprites::Output) -> anyhow::Result<FileChunk> {
    if output.exit_code != 0 {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();

        return Err(anyhow::anyhow!(
            "{}",
            if stderr.is_empty() {
                format!(
                    "reading sandbox file failed with status {}",
                    output.exit_code
                )
            } else {
                stderr.to_string()
            }
        ));
    }

    Ok(FileChunk {
        offset: offset + output.stdout.len() as u64,
        bytes: output.stdout,
    })
}

/// Kills the exec's session and writes its `exit` file. Returns `false` if the
/// exec had already exited or its session was gone.
pub async fn cancel_exec(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    exec_id: models::Id,
) -> anyhow::Result<bool> {
    anyhow::ensure!(
        sandbox.baseline_checkpoint_id.is_some(),
        "sandbox is not ready"
    );
    let exit_path = ExecFile::Exit.path(exec_id);
    if client
        .read_file(&sandbox.handle, SPRITE_HOME, &exit_path)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    let session_id = client
        .read_file(
            &sandbox.handle,
            SPRITE_HOME,
            &format!("{EXEC_DIR}/{exec_id}/session_id"),
        )
        .await?
        .with_context(|| format!("exec {exec_id} not found in this sandbox"))?;
    let session_id = std::str::from_utf8(&session_id).context("invalid exec session ID")?;
    let Some(exit_code) = client.kill_exec(&sandbox.handle, session_id).await? else {
        return Ok(false);
    };
    client
        .write_file(
            &sandbox.handle,
            SPRITE_HOME,
            &exit_path,
            format!("{exit_code}\n"),
        )
        .await
        .context("command stopped but recording its exit status failed")?;
    Ok(true)
}

async fn exec_output(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    timeout: std::time::Duration,
) -> anyhow::Result<crate::sprites::Output> {
    anyhow::ensure!(
        sandbox.baseline_checkpoint_id.is_some(),
        "sandbox is not ready"
    );
    client.exec(&sandbox.handle, argv, timeout).await
}

async fn prepare_baseline(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<String> {
    const INSTALL_FLOWCTL: &str = "mkdir -p $HOME/.local/bin \
        && curl -fsSL -o $HOME/.local/bin/flowctl \
        https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux \
        && chmod +x $HOME/.local/bin/flowctl";
    const INSTALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

    let output = client
        .exec(handle, &["bash", "-lc", INSTALL_FLOWCTL], INSTALL_TIMEOUT)
        .await
        .context("installing flowctl into the sandbox")?;

    anyhow::ensure!(
        output.exit_code == 0,
        "installing flowctl into the sandbox failed with exit code {}: {}",
        output.exit_code,
        String::from_utf8_lossy(&output.stderr),
    );

    client
        .create_checkpoint(handle)
        .await
        .context("capturing the sandbox baseline checkpoint")?;

    // Creation reports the ID only in human-readable progress messages.
    // Before handing the sprite to its user, this is its only saved checkpoint.
    let baseline = client
        .list_checkpoints(handle)
        .await
        .context("identifying the sandbox baseline checkpoint")?
        .into_iter()
        .find(|checkpoint| checkpoint.id != "Current")
        .context("new sprite has no baseline checkpoint")?;

    tracing::info!(%handle, "prepared sandbox baseline");
    Ok(baseline.id)
}

/// Restores the baseline checkpoint, which also restarts the sandbox's processes.
pub async fn reset(client: &crate::sprites::Client, sandbox: &Sandbox) -> anyhow::Result<()> {
    let handle = sandbox.handle.as_str();

    let baseline_id = sandbox
        .baseline_checkpoint_id
        .as_deref()
        .context("sandbox is not ready")?;

    let started = std::time::Instant::now();
    client
        .restore_checkpoint(handle, baseline_id)
        .await
        .with_context(|| format!("restoring baseline checkpoint {baseline_id}"))?;

    tracing::info!(
        %handle,
        %sandbox.id,
        restore_secs = started.elapsed().as_secs_f32(),
        "reset sandbox to baseline"
    );

    Ok(())
}

pub async fn delete(
    client: &crate::sprites::Client,
    pool: &sqlx::PgPool,
    sandbox: &Sandbox,
) -> anyhow::Result<()> {
    if let Err(err) = client.delete_sprite(&sandbox.handle).await {
        tracing::warn!(%sandbox.handle, ?err, "failed to delete sandbox sprite");
    }
    sqlx::query!(
        r#"
        delete from internal.sandboxes
        where id = $1
        "#,
        sandbox.id as models::Id,
    )
    .execute(pool)
    .await
    .context("deleting sandbox record")?;

    tracing::info!(%sandbox.handle, %sandbox.id, "deleted sandbox");
    Ok(())
}
