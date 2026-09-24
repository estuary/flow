//! Sandbox records and lifecycle.
//!
//! A sandbox is a Fly.io Sprite (see [`crate::sprites`]) that runs a user's
//! shell commands. `internal.sandboxes` records each one: its owner, the
//! handle the provider knows it by, and the catalog name the user gave it to tell
//! their sandboxes apart. Callers name a sandbox by its catalog name, and every
//! operation resolves that name against the caller, so a caller reaches only
//! sandboxes whose record names them. Exec metadata, output, and exit status
//! live together in the sandbox under `.estuary/exec/<id>`. Operations on an
//! exec take both its id and its sandbox catalog name; authorization uses the sandbox.
//! Reset and delete discard the metadata along with the output.
//!
//! GraphQL operations in `server::public::graphql::sandboxes` use this module.

use anyhow::Context;
use futures::StreamExt;

/// Prefix of every sandbox handle, which separates sandboxes from sprites that
/// Estuary provisions for other purposes.
const HANDLE_PREFIX: &str = "sbx-";

/// Directory under the sandbox user's home directory that holds one
/// subdirectory per exec. [`EXEC_WRAPPER`] and [`LIST_EXECS`]
/// spell the same path in shell, and [`ExecFile::path`] builds the paths
/// clients read.
const EXEC_DIR: &str = ".estuary/exec";
const SPRITE_HOME: &str = "/home/sprite";

/// Runs a user's command with its output captured in the sandbox.
///
/// Invoked as `bash -c EXEC_WRAPPER flow-exec <exec id> <command> <stdin bytes> <metadata JSON>`,
/// so the exec id and command arrive as `$1` and `$2` and need no quoting.
/// Input arrives on stdin. Before launching, the wrapper checks its
/// length, opens the staged file, and unlinks it: the child keeps the descriptor
/// after detachment without leaving credentials in a named input file.
///
/// The wrapper requires a new exec directory to avoid overwriting past output.
///
/// The command and wrapper share the exec session's process group so Fly's
/// native kill endpoint can stop both. Output is redirected to files, while
/// the wrapper announces startup and records the exit status on normal completion.
const EXEC_WRAPPER: &str = r#"
d="$HOME/.estuary/exec/$1"
mkdir -p "$HOME/.estuary/exec" && mkdir "$d" || exit 125
exec 2> "$d/wrapper.err"
trap 'rm -f "$d/stdin"' EXIT
# Stage all input before announcing startup, so detachment cannot truncate it.
(umask 077; cat > "$d/stdin") || exit 125
[ "$(wc -c < "$d/stdin")" -eq "${3:-0}" ] || exit 125
exec 3< "$d/stdin" || exit 125
rm "$d/stdin" || exit 125
# Commands can carry secrets. Persist metadata before starting the job, and
# expose it to listings only after the command has started.
(umask 077; printf '%s\n' "${4:?missing exec metadata}" > "$d/metadata.json") || exit 125
trap - EXIT
bash -lc "$2" <&3 3<&- > "$d/stdout" 2> "$d/stderr" &
exec 3<&-
job=$!
touch "$d/started"
echo started
wait "$job"
status=$?
# Publish the completed status atomically so readers never see an empty file.
echo "$status" > "$d/exit.tmp" && mv "$d/exit.tmp" "$d/exit"
exit "$status"
"#;

/// Only started commands belong in history: failed launches can leave directories. JSON is serialized by Rust, so command text is never
/// interpreted by the shell and embedded newlines stay within a single record.
const LIST_EXECS: &str = r#"
for d in "$HOME"/.estuary/exec/*; do
    # Older execs used their process group file as the startup marker.
    { [ -f "$d/started" ] || [ -f "$d/pgid" ]; } && [ -f "$d/metadata.json" ] || continue
    printf '['
    cat "$d/metadata.json" || exit 1
    printf ','
    if [ -f "$d/exit" ]; then cat "$d/exit" || exit 1; else printf 'null'; fi
    printf ']\n'
done
"#;

/// Reads a file under the sandbox user's home directory from a byte offset,
/// returning at most one chunk of it.
///
/// Invoked as `bash -c READ_FILE flow-read-file <relative path> <offset>
/// <limit>`, so the path and the two numbers arrive as `$1`, `$2` and `$3` and
/// need no quoting. Stdout contains base64-encoded file bytes; stderr says
/// why it could not read them, and its exit status is the outcome: 0 for a
/// read, 3 for a path that does not exist, and 1 for anything else.
///
/// A path that does not exist is an outcome rather than a failure because it
/// is how a client sees a file that is not there yet, such as the `exit` file
/// [`EXEC_WRAPPER`] writes only once the command has finished.
///
/// Base64 keeps file bytes from being mistaken for exec stream tags.
/// `dd` seeks and counts raw bytes; `pipefail` preserves read errors.
const READ_FILE: &str = r#"
set -o pipefail
f="$HOME/$1"
[ -e "$f" ] || exit 3
[ -d "$f" ] && { echo "path is a directory" >&2; exit 1; }
dd if="$f" iflag=skip_bytes,count_bytes bs=65536 skip="$2" count="$3" status=none | base64 --wrap=0
"#;

/// How long reading a sandbox file may take.
const FILE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Bound finite input before sending it to Fly.
const STDIN_MAX_BYTES: usize = 1024 * 1024;

/// Most bytes one [`read_file`] returns. A file longer than this is read over
/// several calls, each starting where the last one ended, which bounds the
/// memory and the response a single read costs.
pub const READ_MAX_BYTES: u64 = 1024 * 1024;

/// How long a launch may wait for the wrapper's announcement. It bounds the
/// request that starts a command, whose response is dropped once the
/// announcement arrives, so the command's own duration is not bounded by it.
const START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// A row of `internal.sandboxes`: the control plane's record of a sandbox.
#[derive(Debug, Clone)]
pub struct Sandbox {
    pub id: models::Id,
    pub user_id: uuid::Uuid,
    /// The provider's name for the sandbox. It travels to Fly and appears in
    /// the sprite's hostname, so it derives from `id` and carries no user
    /// identifier.
    pub handle: String,
    /// Catalog name on which creation was authorized. The provider never sees it.
    pub catalog_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub baseline_checkpoint_id: Option<String>,
}

/// Persisted exec metadata, enriched with the observed exit status when listed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecEvent {
    pub id: models::Id,
    pub command: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// A file [`EXEC_WRAPPER`] writes in an exec's directory, and the one place a
/// command's output and exit status are recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecFile {
    Stdout,
    Stderr,
    /// The recorded exit status. May remain absent after a wrapper failure.
    Exit,
}

impl ExecFile {
    /// Path of this file of exec `exec_id`, relative to the sandbox user's
    /// home directory, as [`read_file`] takes a path. [`EXEC_WRAPPER`] builds
    /// the same directory in shell.
    pub fn path(self, exec_id: models::Id) -> String {
        let name = match self {
            ExecFile::Stdout => "stdout",
            ExecFile::Stderr => "stderr",
            ExecFile::Exit => "exit",
        };
        format!("{EXEC_DIR}/{exec_id}/{name}")
    }
}

/// A read of a sandbox file.
#[derive(Debug)]
pub struct FileChunk {
    /// Bytes of the file from the requested offset, at most [`READ_MAX_BYTES`]
    /// of them. Empty when the file does not exist.
    pub bytes: Vec<u8>,
    /// Byte offset after `bytes`. The next read continues from here.
    pub offset: u64,
    /// Whether the file existed. A file may be created later, so `false` is an
    /// answer rather than a failure.
    pub exists: bool,
}

/// Builds the provider handle from a sandbox ID.
pub fn handle(id: models::Id) -> String {
    format!("{HANDLE_PREFIX}{id}")
}

/// Why a sandbox was not created.
#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    /// A live sandbox already has that name, regardless of owner.
    #[error("a sandbox named {0:?} already exists")]
    NameTaken(String),
    /// The name is not a valid catalog name.
    #[error("invalid catalog name: {0}")]
    InvalidName(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Why a path cannot name a file in a sandbox.
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    /// The path was empty.
    #[error("path must not be empty")]
    Empty,
    /// The path started with `/`.
    #[error("path must not be absolute")]
    Absolute,
    /// The path had a `..` component.
    #[error("path must not contain '..' components")]
    ParentComponent,
    /// The path ended with `/`, so it cannot name a file.
    #[error("path must not end with '/'")]
    TrailingSlash,
}

/// Why a sandbox file read was refused.
#[derive(Debug, thiserror::Error)]
pub enum FileReadError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Checks that `path` names a file under the sandbox user's home directory:
/// relative, with no `..` component, and not ending in `/`. Nothing wider is
/// asked of it for v0. It is not checked against the sandbox itself, so a
/// path that resolves to an existing directory is instead refused by
/// [`READ_FILE`].
fn validate_relative_path(path: &str) -> Result<(), PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    if path.starts_with('/') {
        return Err(PathError::Absolute);
    }
    if path.ends_with('/') {
        return Err(PathError::TrailingSlash);
    }
    if path.split('/').any(|part| part == "..") {
        return Err(PathError::ParentComponent);
    }
    Ok(())
}

/// Creates a sandbox named `catalog_name` for `user_id` and returns once it accepts
/// commands.
///
/// Refuses an invalid catalog name or one any live sandbox already uses.
/// The sandbox is fully prepared when this returns: it runs commands, flowctl
/// is installed, and the baseline that [`reset`] restores exists. An unready
/// record reserves the catalog name without holding a connection during provisioning.
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
        let baseline_checkpoint_id = provision(client, &sandbox.handle).await?;
        let updated = sqlx::query(
            "update internal.sandboxes set baseline_checkpoint_id = $2, updated_at = now() where id = $1",
        )
        .bind(sandbox.id)
        .bind(&baseline_checkpoint_id)
        .execute(pool)
        .await
        .context("marking sandbox ready")?;
        anyhow::ensure!(
            updated.rows_affected() == 1,
            "sandbox record disappeared during provisioning"
        );
        sandbox.baseline_checkpoint_id = Some(baseline_checkpoint_id);
        Ok::<_, anyhow::Error>(())
    }
    .await;

    if let Err(err) = result {
        // Even a provider timeout must release the catalog name for a fresh attempt.
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

/// Fetches sandbox `id` if it is live and `user_id` owns it.
///
/// An unknown, deleted, or foreign id all resolve to `None`, so a caller learns
/// nothing about sandboxes that are not theirs.
pub async fn fetch(
    pool: &sqlx::PgPool,
    id: models::Id,
    user_id: uuid::Uuid,
) -> anyhow::Result<Option<Sandbox>> {
    sqlx::query_as!(
        Sandbox,
        r#"
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at, baseline_checkpoint_id
        from internal.sandboxes
        where id = $1 and user_id = $2
        "#,
        id as models::Id,
        user_id,
    )
    .fetch_optional(pool)
    .await
    .context("fetching sandbox record")
}

/// Fetches sandbox `catalog_name` if it is live and `user_id` owns it.
///
/// An unknown, deleted, or foreign name all resolve to `None`, so a caller learns
/// nothing about sandboxes that are not theirs.
pub async fn fetch_by_catalog_name(
    pool: &sqlx::PgPool,
    catalog_name: &str,
    user_id: uuid::Uuid,
) -> anyhow::Result<Option<Sandbox>> {
    sqlx::query_as!(
        Sandbox,
        r#"
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at, baseline_checkpoint_id
        from internal.sandboxes
        where catalog_name = $1 and user_id = $2
        "#,
        catalog_name,
        user_id,
    )
    .fetch_optional(pool)
    .await
    .context("fetching sandbox record")
}

/// Lists `user_id`'s live sandboxes, newest first.
pub async fn list(pool: &sqlx::PgPool, user_id: uuid::Uuid) -> anyhow::Result<Vec<Sandbox>> {
    sqlx::query_as!(
        Sandbox,
        r#"
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at, baseline_checkpoint_id
        from internal.sandboxes
        where user_id = $1
        order by created_at desc
        "#,
        user_id,
    )
    .fetch_all(pool)
    .await
    .context("listing sandbox records")
}

/// Lists started execs, newest first. The caller has authorized `sandbox`.
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

/// Inserts a live sandbox record named `catalog_name` for `user_id`.
///
/// The catalog-name index arbitrates concurrent creates across all users.
/// Its violation is read back as [`CreateError::NameTaken`].
async fn persist_record(
    conn: &mut sqlx::PgConnection,
    user_id: uuid::Uuid,
    catalog_name: &str,
) -> Result<Sandbox, CreateError> {
    validator::Validate::validate(&models::Name::new(catalog_name))
        .map_err(|err| CreateError::InvalidName(err.to_string()))?;

    // The handle derives from the generated id, so both come from one statement.
    // A flowid renders as sixteen hex characters; `replace` strips the colons
    // of Postgres's macaddr8 text form to match [`handle`].
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

/// Starts `command` in `sandbox` and returns its exec event once the command
/// is running, without waiting for it to finish.
///
/// The wrapper persists metadata before launching the command and creates a
/// startup marker for listings. Fly's session ID is saved alongside it for cancellation.
///
/// The connection to the sandbox closes once the announcement arrives, and
/// nothing in the control plane follows the command afterwards. It runs for as
/// long as it takes, the wrapper writes its output and exit status to the
/// sandbox, and [`read_file`] reads them on request.
///
/// If the connection fails before startup is acknowledged, the command may
/// still be running. Return that uncertainty without probing or replaying it.
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
    anyhow::ensure!(
        stdin.unwrap_or_default().len() <= STDIN_MAX_BYTES,
        "stdin exceeds the {STDIN_MAX_BYTES}-byte limit"
    );
    let event = ExecEvent {
        id,
        command: command.to_owned(),
        requested_at: chrono::Utc::now(),
        exit_code: None,
    };
    let metadata = serde_json::to_string(&event).context("serializing exec metadata")?;
    let exec_id = event.id.to_string();
    let stdin_len = stdin.unwrap_or_default().len().to_string();
    let argv = [
        "bash",
        "-c",
        EXEC_WRAPPER,
        "flow-exec",
        &exec_id,
        command,
        &stdin_len,
        &metadata,
    ];

    match launch(client, sandbox, &argv, stdin).await {
        Ok(session_id) => {
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
        Err(LaunchError::Failed(err)) => Err(err),
        Err(LaunchError::Lost(err)) => Err(err.context("command startup is uncertain")),
    }
}

/// Why [`launch`] could not confirm that a command started.
#[derive(Debug)]
enum LaunchError {
    /// The command did not start: the API refused it, or the wrapper failed
    /// before it launched the command.
    Failed(anyhow::Error),
    /// The connection was lost before the wrapper announced itself, so the
    /// command may or may not have started.
    Lost(anyhow::Error),
}

impl From<crate::sprites::ExecError> for LaunchError {
    fn from(err: crate::sprites::ExecError) -> Self {
        match err {
            crate::sprites::ExecError::Transport(err) => LaunchError::Lost(err),
            crate::sprites::ExecError::NotReady => {
                LaunchError::Failed(anyhow::anyhow!("sandbox is not ready"))
            }
            err @ crate::sprites::ExecError::SpriteMissing => {
                LaunchError::Failed(anyhow::Error::new(err))
            }
            crate::sprites::ExecError::Other(err) => LaunchError::Failed(err),
        }
    }
}

/// Starts the wrapped command `argv` in `sandbox` and returns once the wrapper
/// announces that the command is running. The connection closes when this
/// returns, and the command runs on without it.
///
/// The wrapper saves stdin to a file before announcing startup, so the command
/// can read its input after this function closes the API connection.
///
/// A wrapper that writes to stderr or exits before announcing itself did not
/// start the command: the only stderr it sends over the connection comes
/// before it redirects that stream, from a directory it could not create.
async fn launch(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    stdin: Option<&str>,
) -> Result<String, LaunchError> {
    let mut frames = client
        .exec_stream(&sandbox.handle, argv, stdin, START_TIMEOUT)
        .await?;

    let mut session_id = None;
    let mut started = false;
    while let Some(frame) = frames.next().await {
        match frame
            .map_err(|err| LaunchError::Lost(err.context("waiting for the command to start")))?
        {
            crate::sprites::Frame::SessionInfo { session_id: id } => session_id = Some(id),
            crate::sprites::Frame::Stdout(_) => started = true,
            crate::sprites::Frame::Stderr(message) => {
                return Err(LaunchError::Failed(anyhow::anyhow!(
                    "command failed to start: {}",
                    String::from_utf8_lossy(&message).trim()
                )));
            }
            crate::sprites::Frame::Exit(status) => {
                return Err(LaunchError::Failed(anyhow::anyhow!(
                    "command wrapper exited with status {status} before startup was confirmed"
                )));
            }
        }
        if started && let Some(id) = session_id.take() {
            return Ok(id);
        }
    }
    Err(LaunchError::Lost(anyhow::anyhow!(
        "exec stream ended before startup and session ID were received"
    )))
}

/// Reads `path` in `sandbox`, resolved against the sandbox user's home
/// directory, from byte `offset` onwards.
///
/// `path` must be relative and free of `..` components; see
/// [`validate_relative_path`]. `limit` bounds the bytes one read returns, and
/// is itself bounded by [`READ_MAX_BYTES`], which `None` asks for: a client
/// reading a longer file continues from the offset it is handed. A path that
/// does not exist is reported rather than raised: see [`READ_FILE`].
///
/// This is a file operation, not a command a user ran: unlike [`exec`],
/// it creates no exec metadata. A command's
/// output and exit status are read this way, at the paths [`ExecFile`] builds.
pub async fn read_file(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    path: &str,
    offset: u64,
    limit: Option<u64>,
) -> Result<FileChunk, FileReadError> {
    validate_relative_path(path)?;

    let offset_arg = offset.to_string();
    // The ceiling holds whatever a caller asks for, so the bound on a response
    // is this crate's and not the client's.
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

/// Decodes successful [`READ_FILE`] output. Exit status 3 means the path is
/// missing; other nonzero statuses report a read failure.
fn chunk_from_output(
    offset: u64,
    output: crate::sprites::Output,
) -> Result<FileChunk, FileReadError> {
    if output.exit_code == 3 {
        return Ok(FileChunk {
            bytes: Vec::new(),
            offset,
            exists: false,
        });
    }

    if output.exit_code != 0 {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();

        return Err(FileReadError::Other(anyhow::anyhow!(
            "{}",
            if stderr.is_empty() {
                format!(
                    "reading sandbox file failed with status {}",
                    output.exit_code
                )
            } else {
                stderr.to_string()
            }
        )));
    }

    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &output.stdout)
        .context("decoding sandbox file bytes")?;

    Ok(FileChunk {
        offset: offset + bytes.len() as u64,
        bytes,
        exists: true,
    })
}

/// Stops the native session and records its final status alongside its output.
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
        .await?;
    let Some(session_id) = session_id else {
        let metadata = client
            .read_file(
                &sandbox.handle,
                SPRITE_HOME,
                &format!("{EXEC_DIR}/{exec_id}/metadata.json"),
            )
            .await?;
        anyhow::ensure!(
            metadata.is_some(),
            "exec {exec_id} not found in this sandbox"
        );
        anyhow::bail!("exec {exec_id} has no saved cancellation session ID");
    };
    let session_id = std::str::from_utf8(&session_id).context("invalid exec session ID")?;
    let Some(exit_code) = client.kill_exec(&sandbox.handle, session_id.trim()).await? else {
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

/// Runs `argv` in `sandbox` and collects its output.
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

/// Creates and bootstraps `handle`'s sprite.
async fn provision(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<String> {
    client.create_sprite(handle).await?;
    let result = bootstrap(client, handle).await;

    // A timeout leaves the provider's state uncertain. Leave that sprite for
    // orphan cleanup rather than racing an operation that may still be running.
    // TODO: Add an orphan reaper for sandbox sprites without database records.
    if let Err(err) = &result
        && !err.chain().any(|cause| {
            cause
                .downcast_ref::<reqwest::Error>()
                .is_some_and(reqwest::Error::is_timeout)
        })
    {
        if let Err(err) = client.delete_sprite(handle).await {
            tracing::warn!(%handle, ?err, "failed to delete an incompletely provisioned sandbox");
        }
    }
    result
}

/// Prepares a just-provisioned sandbox by installing flowctl and capturing
/// the baseline that [`reset`] restores.
///
/// Every step must succeed, since a caller hands its user a prepared sandbox or
/// none at all.
async fn bootstrap(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<String> {
    const INSTALL_FLOWCTL: &str = "mkdir -p $HOME/.local/bin \
        && curl -fsSL -o $HOME/.local/bin/flowctl.download \
        https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux \
        && chmod +x $HOME/.local/bin/flowctl.download \
        && mv $HOME/.local/bin/flowctl.download $HOME/.local/bin/flowctl";
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
    let mut checkpoints = client
        .list_checkpoints(handle)
        .await
        .context("identifying the sandbox baseline checkpoint")?
        .into_iter()
        .filter(|checkpoint| checkpoint.id != "Current");
    let baseline = checkpoints
        .next()
        .context("new sprite has no baseline checkpoint")?;
    anyhow::ensure!(
        checkpoints.next().is_none(),
        "new sprite has multiple checkpoints"
    );

    tracing::info!(%handle, "bootstrapped sandbox");
    Ok(baseline.id)
}

/// Resets `sandbox` to its provisioning baseline, returning once the restore
/// has completed.
///
/// Restoring reverts the filesystem and restarts the sandbox's processes from
/// the baseline, discarding exec metadata and output together.
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

/// Deletes `sandbox`: its sprite and storage at the provider, then its record
/// (exec metadata is deleted with the storage).
///
/// Delete the sprite first. For ready sandboxes, provider failure preserves
/// the record. For unready sandboxes, cleanup is best-effort so a stuck
/// provisioning attempt cannot keep its catalog name reserved.
pub async fn delete(
    client: &crate::sprites::Client,
    pool: &sqlx::PgPool,
    sandbox: &Sandbox,
) -> anyhow::Result<()> {
    if let Err(err) = client.delete_sprite(&sandbox.handle).await {
        if sandbox.baseline_checkpoint_id.is_some() {
            return Err(err.context("deleting sprite"));
        }
        tracing::warn!(%sandbox.handle, ?err, "failed to clean up an unready sandbox sprite");
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
