//! Sandbox records and lifecycle.
//!
//! A sandbox is a Fly.io Sprite (see [`crate::sprites`]) that runs a user's
//! shell commands. `internal.sandboxes` records each one: its owner, the
//! handle the provider knows it by, and the catalog name the user gave it to tell
//! their sandboxes apart. Callers name a sandbox by its record id, and every
//! operation resolves that id against the caller, so a caller reaches only
//! sandboxes whose record names them. Exec metadata, output, and exit status
//! live together in the sandbox under `.estuary/exec/<id>`. Operations on an
//! exec take both its id and its sandbox id; authorization uses the sandbox.
//! Deletion discards the metadata along with the output.
//!
//! GraphQL operations in `server::public::graphql::sandboxes` use this module.

use anyhow::Context;
use futures::StreamExt;

/// Prefix of every sandbox handle, which separates sandboxes from sprites that
/// Estuary provisions for other purposes.
const HANDLE_PREFIX: &str = "sbx-";

/// Directory under the sandbox user's home directory that holds one
/// subdirectory per exec. [`EXEC_WRAPPER`] and [`PROBE_START`]
/// spell the same path in shell, and [`ExecFile::path`] builds the paths
/// clients read.
const EXEC_DIR: &str = ".estuary/exec";

/// Runs a user's command with its output captured in the sandbox.
///
/// Invoked as `bash -c EXEC_WRAPPER flow-exec <exec id> <command> <stdin bytes> <metadata JSON>`,
/// so the exec id and command arrive as `$1` and `$2` and need no quoting.
/// Input arrives in the HTTP body. Before launching, the wrapper checks its
/// length, opens the staged file, and unlinks it: the child keeps the descriptor
/// after detachment without leaving credentials in a named input file.
///
/// The wrapper creates the exec's directory, which must not exist yet:
/// [`PROBE_START`] creates it to claim an exec whose launch was abandoned, and
/// a wrapper that finds the directory taken exits without running the command.
///
/// The command runs under a login shell as a background job, with stdout and
/// stderr redirected to files. Job control (`set -m`) gives that job a process
/// group of its own. The wrapper records it as the marker that the command
/// started, which [`PROBE_START`] looks for.
///
/// The wrapper then announces itself with one line on its own stdout, because
/// the Sprites API sends its response only once the command writes something
/// and the user's command writes only to files: without this line a caller
/// starting a command would wait for it to exit. The caller drops the
/// connection once the line arrives, and the wrapper runs on without it: it
/// waits for the job, writes its exit status, and exits with it. Job control
/// also reports a job's end on the wrapper's stderr, where a caller starting a
/// command would read it as a failure to start, so the wrapper's own stderr
/// goes to a file.
const EXEC_WRAPPER: &str = r#"
d="$HOME/.estuary/exec/$1"
mkdir -p "$HOME/.estuary/exec" && mkdir "$d" || exit 125
exec 2> "$d/wrapper.err"
trap 'rm -f "$d/stdin"; touch "$d/unstarted"' EXIT
# Stage all input before announcing startup, so detachment cannot truncate it.
(umask 077; cat > "$d/stdin") || exit 125
[ "$(wc -c < "$d/stdin")" -eq "${3:-0}" ] || exit 125
exec 3< "$d/stdin" || exit 125
rm "$d/stdin" || exit 125
# Commands can carry secrets. Persist metadata before starting the job, and
# expose it to listings only after the process group has been recorded.
(umask 077; printf '%s\n' "${4:?missing exec metadata}" > "$d/metadata.json") || exit 125
trap - EXIT
set -m
bash -lc "$2" <&3 3<&- > "$d/stdout" 2> "$d/stderr" &
exec 3<&-
job=$!
echo "$job" > "$d/pgid"
echo started
wait "$job"
status=$?
# Publish the completed status atomically so readers never see an empty file.
echo "$status" > "$d/exit.tmp" && mv "$d/exit.tmp" "$d/exit"
exit "$status"
"#;

/// Only started commands belong in history: probes and failed launches can
/// also leave directories. JSON is serialized by Rust, so command text is never
/// interpreted by the shell and embedded newlines stay within a single record.
const LIST_EXECS: &str = r#"
for d in "$HOME"/.estuary/exec/*; do
    [ -f "$d/pgid" ] && [ -f "$d/metadata.json" ] || continue
    printf '['
    cat "$d/metadata.json" || exit 1
    printf ','
    if [ -f "$d/exit" ]; then cat "$d/exit" || exit 1; else printf 'null'; fi
    printf ']\n'
done
"#;

/// Settles whether the command of an exec started, for a launch whose
/// connection was lost before the wrapper announced itself.
///
/// Invoked as `bash -c PROBE_START flow-probe-start <exec id>`. The process
/// group [`EXEC_WRAPPER`] records is the marker that the command started. When
/// it is absent the probe claims the exec directory: the wrapper refuses a
/// directory that already exists, so a claim that succeeds means the command
/// has not started and now cannot, and one that fails means the wrapper is
/// staging input or recording the group. An `unstarted` marker means input
/// staging failed and the wrapper cannot launch. Exactly one of them creates the
/// directory, so the probe can fence off a late launch without replaying it.
/// Existing directories currently reject repeated launches under the same id;
/// they do not return the original execution to a retrying caller.
///
/// The script prints `started` or `unstarted` on stdout and exits 0. It exits
/// 1 with the reason on stderr when the directory exists but the group never
/// appears, which means the wrapper died mid-start.
const PROBE_START: &str = r#"
d="$HOME/.estuary/exec/$1"
if [ -e "$d/pgid" ]; then echo started; exit 0; fi
mkdir -p "$HOME/.estuary/exec"
if mkdir "$d" 2>/dev/null; then echo unstarted; exit 0; fi
for _ in $(seq 50); do
    [ -e "$d/pgid" ] && { echo started; exit 0; }
    [ -e "$d/unstarted" ] && { echo unstarted; exit 0; }
    sleep 0.1
done
echo "exec directory exists but the command's process group was never recorded" >&2
exit 1
"#;

/// Reads a file under the sandbox user's home directory from a byte offset,
/// returning at most one chunk of it.
///
/// Invoked as `bash -c READ_FILE flow-read-file <relative path> <offset>
/// <limit>`, so the path and the two numbers arrive as `$1`, `$2` and `$3` and
/// need no quoting. The script's stdout is the file's bytes, its stderr says
/// why it could not read them, and its exit status is the outcome: 0 for a
/// read, 3 for a path that does not exist, and 1 for anything else.
///
/// A path that does not exist is an outcome rather than a failure because it
/// is how a client sees a file that is not there yet, such as the `exit` file
/// [`EXEC_WRAPPER`] writes only once the command has finished.
///
/// `dd` performs the read in one process, so its exit status is the read's own
/// and a bounded read of a long file ends by count. It seeks and counts in
/// bytes with `skip_bytes,count_bytes`, reads in whole blocks, suppresses its
/// transfer summary with `status=none`, and exits non-zero with its reason on
/// stderr. A `skip` past the end of the file reads nothing and succeeds.
const READ_FILE: &str = r#"
f="$HOME/$1"
[ -e "$f" ] || exit 3
[ -d "$f" ] && { echo "path is a directory" >&2; exit 1; }
dd if="$f" iflag=skip_bytes,count_bytes bs=65536 skip="$2" count="$3" status=none
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

/// How long settling whether a command started may take. [`PROBE_START`]
/// waits up to five seconds for a wrapper it found mid-start.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long an operation waits out a sprite that is not ready to run commands.
/// A suspended sprite wakes in well under a second and a cold one in a few, so
/// this leaves room for both while still failing a sprite that is stuck. See
/// [`while_waking`].
const WAKE_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

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
}

/// Persisted exec metadata, enriched with the observed exit status when listed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecEvent {
    pub id: models::Id,
    pub command: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_result: Option<i32>,
}

/// A file [`EXEC_WRAPPER`] writes in an exec's directory, and the one place a
/// command's output and exit status are recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecFile {
    Stdout,
    Stderr,
    /// The command's exit status, written once it has finished and absent
    /// until then. Its appearance is how a client knows the command is done.
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

/// Builds the handle of the sandbox recorded under `id`. The insert in
/// [`insert_record`] computes the same value in SQL.
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
/// Refuses an invalid catalog name, one any live sandbox
/// already uses. The sandbox is fully prepared
/// when this returns: it runs commands and flowctl is installed. If creating
/// the sprite fails the record stays, and the sandbox's first command
/// provisions the sprite instead.
pub async fn create(
    client: &crate::sprites::Client,
    pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
    catalog_name: &str,
) -> Result<Sandbox, CreateError> {
    let sandbox = insert_record(pool, user_id, catalog_name).await?;

    client.create_sprite(&sandbox.handle).await?;
    wait_until_ready(client, &sandbox.handle).await?;

    // Discard an incomplete installation so the sandbox's next command can
    // provision and bootstrap a fresh sprite using the retained record.
    if let Err(err) = bootstrap(client, &sandbox.handle).await {
        if let Err(err) = client.delete_sprite(&sandbox.handle).await {
            tracing::warn!(handle = %sandbox.handle, ?err, "failed to delete a sandbox that did not bootstrap");
        }
        return Err(err.into());
    }

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
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at
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
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at
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
        select id as "id!: models::Id", user_id, handle, catalog_name, created_at
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
            result.map(|(mut event, exit_result)| {
                event.exit_result = exit_result;
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
pub(crate) async fn insert_record(
    pool: &sqlx::PgPool,
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
        returning id as "id!: models::Id", user_id, handle, catalog_name, created_at
        "#,
        user_id,
        HANDLE_PREFIX,
        catalog_name,
    )
    .fetch_one(pool)
    .await
    .map_err(|err| match err.as_database_error() {
        Some(db) if db.constraint() == Some("sandboxes_catalog_name_idx") => {
            CreateError::NameTaken(catalog_name.to_string())
        }
        _ => CreateError::Other(anyhow::Error::new(err).context("creating sandbox record")),
    })?;

    tracing::info!(%sandbox.id, %sandbox.handle, %user_id, "created sandbox record");
    Ok(sandbox)
}

/// Starts `command` in `sandbox` and returns its exec event once the command
/// is running, without waiting for it to finish.
///
/// The wrapper persists metadata before launching the command. Listings only
/// include directories with a process group marker, so failed launches do not
/// appear in history. "Running" is the wrapper's announcement
/// (see [`EXEC_WRAPPER`]), which follows its process group file, so a read
/// issued after this returns finds the exec.
///
/// The connection to the sandbox closes once the announcement arrives, and
/// nothing in the control plane follows the command afterwards. It runs for as
/// long as it takes, the wrapper writes its output and exit status to the
/// sandbox, and [`read_file`] reads them on request.
///
/// A connection lost before the announcement leaves the launch in doubt. The
/// sandbox settles it (see [`PROBE_START`]) rather than the command being
/// retried, which could run it twice. When the probe cannot settle it either,
/// its metadata remains in the sandbox for a later listing to discover.
pub async fn exec(
    client: &crate::sprites::Client,
    id: models::Id,
    sandbox: &Sandbox,
    command: &str,
    stdin: Option<&str>,
) -> anyhow::Result<ExecEvent> {
    anyhow::ensure!(
        stdin.unwrap_or_default().len() <= STDIN_MAX_BYTES,
        "stdin exceeds the {STDIN_MAX_BYTES}-byte limit"
    );
    let event = ExecEvent {
        id,
        command: command.to_owned(),
        requested_at: chrono::Utc::now(),
        exit_result: None,
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

    let err = match launch(client, sandbox, &argv, stdin).await {
        Ok(()) => return Ok(event),
        Err(LaunchError::Failed(err)) => err,
        Err(LaunchError::Lost(err)) => match probe_started(client, sandbox, &exec_id).await {
            Ok(true) => {
                tracing::warn!(%sandbox.id, %event.id, ?err, "lost the exec connection after the command started");
                return Ok(event);
            }
            Ok(false) => err.context("the command did not start"),
            Err(probe_err) => {
                return Err(err.context(format!(
                    "could not determine whether the command started: {probe_err:#}"
                )));
            }
        },
    };

    Err(err)
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
/// A wrapper that writes to stderr or exits before announcing itself did not
/// start the command: the only stderr it sends over the connection comes
/// before it redirects that stream, from a directory it could not create.
async fn launch(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    stdin: Option<&str>,
) -> Result<(), LaunchError> {
    let mut frames = start(client, sandbox, argv, stdin, START_TIMEOUT).await?;

    match frames.next().await {
        Some(Ok(crate::sprites::Frame::Stdout(_announcement))) => Ok(()),
        Some(Ok(crate::sprites::Frame::Stderr(message))) => {
            Err(LaunchError::Failed(anyhow::anyhow!(
                "command failed to start: {}",
                String::from_utf8_lossy(&message).trim()
            )))
        }
        Some(Ok(crate::sprites::Frame::Exit(status))) => Err(LaunchError::Failed(anyhow::anyhow!(
            "command wrapper exited with status {status} before starting"
        ))),
        Some(Err(err)) => Err(LaunchError::Lost(
            err.context("waiting for the command to start"),
        )),
        None => Err(LaunchError::Lost(anyhow::anyhow!(
            "exec stream ended before the command started"
        ))),
    }
}

/// Settles whether the command of exec `exec_id` in `sandbox` started, for a
/// launch whose connection was lost first. See [`PROBE_START`].
async fn probe_started(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    exec_id: &str,
) -> anyhow::Result<bool> {
    let argv = ["bash", "-c", PROBE_START, "flow-probe-start", exec_id];

    let output = exec_output(client, sandbox, &argv, PROBE_TIMEOUT)
        .await
        .context("probing whether the command started")?;

    started_from_output(output)
}

/// Interprets the output of [`PROBE_START`]: `started` or `unstarted` on its
/// stdout, or its stderr says why it could not tell.
fn started_from_output(output: crate::sprites::Output) -> anyhow::Result<bool> {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();

    if output.exit_code != 0 {
        anyhow::bail!(
            "{}",
            if stderr.is_empty() {
                format!(
                    "probing whether the command started failed with status {}",
                    output.exit_code
                )
            } else {
                stderr.to_string()
            }
        );
    }

    match String::from_utf8_lossy(&output.stdout).trim() {
        "started" => Ok(true),
        "unstarted" => Ok(false),
        other => anyhow::bail!("probing whether the command started reported {other:?}"),
    }
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

/// Interprets the exit of [`READ_FILE`]: its stdout is the file's bytes, a
/// status of 3 is a path that does not exist, and anything else is a failure
/// its stderr explains.
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

    Ok(FileChunk {
        offset: offset + output.stdout.len() as u64,
        bytes: output.stdout,
        exists: true,
    })
}

/// Starts `argv` in `sandbox`.
///
/// A record whose sprite is missing, because [`create`] failed after inserting
/// it, is provisioned here: the Sprites API reports an absent sprite
/// distinctly, so this creates one, bootstraps it, and retries.
async fn start(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    stdin: Option<&str>,
    timeout: std::time::Duration,
) -> Result<futures::stream::BoxStream<'static, anyhow::Result<crate::sprites::Frame>>, LaunchError>
{
    let handle = sandbox.handle.as_str();
    let attempt = || client.exec_stream_with_stdin(handle, argv, stdin, timeout);

    match while_waking(handle, attempt).await {
        Ok(frames) => Ok(frames),
        Err(crate::sprites::ExecError::SpriteMissing) => {
            tracing::info!(%handle, %sandbox.id, "provisioning a sandbox whose sprite is missing");
            provision(client, handle)
                .await
                .map_err(LaunchError::Failed)?;

            while_waking(handle, attempt)
                .await
                .map_err(LaunchError::from)
        }
        Err(err @ crate::sprites::ExecError::NotReady) => {
            client.log_unavailable_state(handle).await;
            Err(err.into())
        }
        Err(err) => Err(err.into()),
    }
}

/// Runs `argv` in `sandbox` and returns its whole output, for the operations
/// that want a result rather than progress.
///
/// This waits out a waking sprite, as [`start`] does. It does not provision a
/// sprite that is missing: creating and bootstrapping one takes seconds and
/// yields an empty filesystem, which answers nothing an operation on existing
/// state asked. Only [`start`], which is launching work, goes that far.
async fn exec_output(
    client: &crate::sprites::Client,
    sandbox: &Sandbox,
    argv: &[&str],
    timeout: std::time::Duration,
) -> anyhow::Result<crate::sprites::Output> {
    let frames = match while_waking(&sandbox.handle, || {
        client.exec_stream(&sandbox.handle, argv, timeout)
    })
    .await
    {
        Err(err @ crate::sprites::ExecError::NotReady) => {
            client.log_unavailable_state(&sandbox.handle).await;
            return Err(err.into());
        }
        result => result?,
    };

    crate::sprites::fold_output(frames).await
}

/// Repeats `attempt` while the sprite answers that it is not ready to run
/// commands.
///
/// HTTP 503 may be a transient wake transition, but also a provider failure.
/// Retry only that explicit refusal, and record the outcome so sustained
/// failures can be distinguished from short recoveries.
async fn while_waking<T, F, Fut>(
    handle: &str,
    mut attempt: F,
) -> Result<T, crate::sprites::ExecError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::sprites::ExecError>>,
{
    const FIRST_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

    let start = std::time::Instant::now();
    let mut backoff = FIRST_BACKOFF;
    let mut attempts = 0;

    loop {
        attempts += 1;
        match attempt().await {
            Err(crate::sprites::ExecError::NotReady) if start.elapsed() + backoff < WAKE_BUDGET => {
                tracing::debug!(
                    sprite = handle,
                    attempts,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    backoff_ms = backoff.as_millis() as u64,
                    "retrying Sprites exec after HTTP 503"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            result => {
                if attempts > 1 || matches!(&result, Err(crate::sprites::ExecError::NotReady)) {
                    let outcome = match &result {
                        Ok(_) => "recovered",
                        Err(crate::sprites::ExecError::NotReady) => "budget_exhausted",
                        Err(_) => "other_error",
                    };
                    tracing::info!(
                        sprite = handle,
                        attempts,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        outcome,
                        "Sprites exec retry sequence ended"
                    );
                }
                return result;
            }
        }
    }
}

/// Creates `handle`'s sprite, waits for it to accept commands, and bootstraps
/// it. An existing sprite of that name counts as created.
async fn provision(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<()> {
    client.create_sprite(handle).await?;
    wait_until_ready(client, handle).await?;
    bootstrap(client, handle).await
}

/// Waits for a just-created sprite to accept commands.
///
/// `create_sprite` returns before the sprite's VM has booted, so exec against it
/// fails with [`crate::sprites::ExecError::NotReady`] (HTTP 503) until the boot
/// completes. This probes with a trivial command, backing off between tries,
/// until the sprite accepts one or the budget elapses. A probe returning `Ok`
/// means the exec endpoint accepted the command, which is the readiness signal;
/// the probe command's own output is irrelevant, so its stream is dropped.
async fn wait_until_ready(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<()> {
    const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(180);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);

    let start = std::time::Instant::now();
    let mut backoff = std::time::Duration::from_secs(1);

    loop {
        match client.exec_stream(handle, &["true"], PROBE_TIMEOUT).await {
            Ok(_frames) => return Ok(()),
            Err(crate::sprites::ExecError::NotReady) => {
                anyhow::ensure!(
                    start.elapsed() < BUDGET,
                    "sandbox did not become ready within {}s",
                    BUDGET.as_secs()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(err) => {
                return Err(anyhow::Error::new(err).context("waiting for sandbox to become ready"));
            }
        }
    }
}

/// Prepares a just-provisioned sandbox by installing flowctl.
///
/// Every step must succeed, since a caller hands its user a prepared sandbox or
/// none at all.
async fn bootstrap(client: &crate::sprites::Client, handle: &str) -> anyhow::Result<()> {
    // Concurrent commands can each provision a sandbox whose sprite went
    // missing, so the binary lands under its final name only once it is
    // complete.
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

    tracing::info!(%handle, "bootstrapped sandbox");
    Ok(())
}

/// Deletes `sandbox`: its sprite and storage at the provider, then its record
/// (exec metadata is deleted with the storage).
///
/// The provider call goes first. If it fails the record stays live and the
/// caller can retry; the reverse order could delete a record while leaving an
/// orphan sprite that nothing references.
pub async fn delete(
    client: &crate::sprites::Client,
    pool: &sqlx::PgPool,
    sandbox: &Sandbox,
) -> anyhow::Result<()> {
    client
        .delete_sprite(&sandbox.handle)
        .await
        .context("deleting sprite")?;
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::sprites::Output;

    const ALICE: uuid::Uuid = uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    const BOB: uuid::Uuid = uuid::Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);

    #[test]
    fn test_wrapper_stdin_is_literal_and_unlinked() {
        use std::io::Write;

        for input in [
            "",
            "invented-token='abc'\n$(touch injected) `touch injected`\n雪\0tail",
        ] {
            let home = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new("bash")
                .args([
                    "-c",
                    EXEC_WRAPPER,
                    "flow-exec",
                    "input",
                    "cat",
                    &input.len().to_string(),
                    r#"{"id":"0102030405060708","command":"cat","requested_at":"2026-09-21T00:00:00Z"}"#,
                ])
                .env("HOME", home.path())
                .current_dir(home.path())
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            let dir = home.path().join(".estuary/exec/input");
            assert_eq!(std::fs::read(dir.join("stdout")).unwrap(), input.as_bytes());
            assert_eq!(output.stdout, b"started\n");
            assert!(!dir.join("stdin").exists());
            assert!(!home.path().join("injected").exists());
        }
    }

    /// Round-trip shell-sensitive metadata, filter incomplete launches, and
    /// order by request time rather than directory traversal order.
    #[test]
    fn test_exec_metadata_round_trip_and_listing() {
        let home = tempfile::tempdir().unwrap();
        let command = ": # quotes ' \" $(touch injected) `touch injected`\n# 雪\n";
        for (id, requested_at) in [
            ("0102030405060708", "2026-09-21T00:00:02Z"),
            ("0102030405060709", "2026-09-21T00:00:01Z"),
        ] {
            let metadata =
                serde_json::json!({"id": id, "command": command, "requested_at": requested_at})
                    .to_string();
            let output = std::process::Command::new("bash")
                .args(["-c", EXEC_WRAPPER, "flow-exec", id, command, "0", &metadata])
                .env("HOME", home.path())
                .current_dir(home.path())
                .stdin(std::process::Stdio::null())
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            // Reusing a directory must not overwrite a prior execution's
            // metadata or start another command under its identity.
            let repeated = std::process::Command::new("bash")
                .args([
                    "-c",
                    EXEC_WRAPPER,
                    "flow-exec",
                    id,
                    "touch duplicate",
                    "0",
                    "{}",
                ])
                .env("HOME", home.path())
                .current_dir(home.path())
                .stdin(std::process::Stdio::null())
                .output()
                .unwrap();
            assert_eq!(repeated.status.code(), Some(125));
            assert!(!home.path().join("duplicate").exists());
            let path = home
                .path()
                .join(format!(".estuary/exec/{id}/metadata.json"));
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                format!("{metadata}\n")
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
        assert!(!home.path().join("injected").exists());
        let root = home.path().join(".estuary/exec");
        for name in ["probe", "staging", "legacy"] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        std::fs::write(
            root.join("staging/metadata.json"),
            "invalid but not started",
        )
        .unwrap();
        std::fs::write(root.join("legacy/pgid"), "123").unwrap();
        let listed = std::process::Command::new("bash")
            .args(["-c", LIST_EXECS])
            .env("HOME", home.path())
            .output()
            .unwrap();
        assert!(listed.status.success(), "{listed:?}");
        let events = execs_from_metadata(&listed.stdout).unwrap();
        insta::assert_json_snapshot!(events, @r###"
        [
          {
            "id": "0102030405060708",
            "command": ": # quotes ' \" $(touch injected) `touch injected`\n# 雪\n",
            "requested_at": "2026-09-21T00:00:02Z",
            "exit_result": 0
          },
          {
            "id": "0102030405060709",
            "command": ": # quotes ' \" $(touch injected) `touch injected`\n# 雪\n",
            "requested_at": "2026-09-21T00:00:01Z",
            "exit_result": 0
          }
        ]
        "###);
        std::fs::remove_file(root.join("0102030405060708/exit")).unwrap();
        std::fs::write(root.join("0102030405060709/exit"), "137\n").unwrap();
        let listed = std::process::Command::new("bash")
            .args(["-c", LIST_EXECS])
            .env("HOME", home.path())
            .output()
            .unwrap();
        assert!(listed.status.success(), "{listed:?}");
        let results: Vec<_> = execs_from_metadata(&listed.stdout)
            .unwrap()
            .into_iter()
            .map(|event| event.exit_result)
            .collect();
        insta::assert_json_snapshot!(results, @r#"
        [
          null,
          137
        ]
        "#);
        assert!(execs_from_metadata(b"{broken}").is_err());
        std::fs::remove_dir_all(root).unwrap();
        let empty = std::process::Command::new("bash")
            .args(["-c", LIST_EXECS])
            .env("HOME", home.path())
            .output()
            .unwrap();
        assert!(empty.status.success());
        assert!(execs_from_metadata(&empty.stdout).unwrap().is_empty());
    }

    #[test]
    fn test_wrapper_rejects_truncated_stdin() {
        let home = tempfile::tempdir().unwrap();
        let output = std::process::Command::new("bash")
            .args([
                "-c", EXEC_WRAPPER, "flow-exec", "input", "touch ran", "10",
                r#"{"id":"0102030405060708","command":"touch ran","requested_at":"2026-09-21T00:00:00Z"}"#,
            ])
            .env("HOME", home.path())
            .current_dir(home.path())
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(125));
        assert!(output.stdout.is_empty());
        assert!(!home.path().join("ran").exists());
        assert!(!home.path().join(".estuary/exec/input/stdin").exists());
        assert!(
            !home
                .path()
                .join(".estuary/exec/input/metadata.json")
                .exists()
        );
        let probe = std::process::Command::new("bash")
            .args(["-c", PROBE_START, "flow-probe-start", "input"])
            .env("HOME", home.path())
            .output()
            .unwrap();
        assert!(probe.status.success());
        assert_eq!(probe.stdout, b"unstarted\n");
    }

    #[test]
    fn test_handles_derive_from_the_record() {
        let id = models::Id::from_hex("0102030405060708").unwrap();
        insta::assert_snapshot!(handle(id), @"sbx-0102030405060708");
    }

    /// A sprite that is waking refuses commands and then accepts them, so a
    /// caller must see the eventual success and not the refusal. Every other
    /// error is the sprite's answer and returns at once, since waiting would
    /// not change it.
    #[tokio::test]
    async fn test_while_waking_retries_only_a_not_ready_sprite() {
        let attempts = std::cell::Cell::new(0);
        let waking = || {
            attempts.set(attempts.get() + 1);
            let attempt = attempts.get();
            async move {
                if attempt <= 2 {
                    Err(crate::sprites::ExecError::NotReady)
                } else {
                    Ok(attempt)
                }
            }
        };

        assert_eq!(while_waking("test", waking).await.unwrap(), 3);
        assert_eq!(attempts.get(), 3);

        let attempts = std::cell::Cell::new(0);
        let missing = || {
            attempts.set(attempts.get() + 1);
            async { Err::<(), _>(crate::sprites::ExecError::SpriteMissing) }
        };

        assert!(matches!(
            while_waking("test", missing).await,
            Err(crate::sprites::ExecError::SpriteMissing)
        ));
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn test_exec_file_paths() {
        let id = models::Id::from_hex("0102030405060708").unwrap();
        let cases = [ExecFile::Stdout, ExecFile::Stderr, ExecFile::Exit];

        insta::assert_debug_snapshot!(
            cases
                .into_iter()
                .map(|file| file.path(id))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_chunk_from_output() {
        let output = |stdout: &[u8], stderr: &[u8], exit_code: i32| Output {
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
            exit_code,
        };
        let render = |chunk: Result<FileChunk, FileReadError>| match chunk {
            Ok(chunk) => format!(
                "{:?} offset={} exists={}",
                String::from_utf8_lossy(&chunk.bytes),
                chunk.offset,
                chunk.exists
            ),
            Err(err) => format!("error: {err:#}"),
        };

        let cases = [
            // Bytes from the requested offset, which the next offset follows.
            chunk_from_output(10, output(b"hello\n", b"", 0)),
            // A read that starts at the end of the file holds the offset.
            chunk_from_output(20, output(b"", b"", 0)),
            // The file is not there, which a later read may still find.
            chunk_from_output(0, output(b"", b"", 3)),
            // The path names a directory.
            chunk_from_output(0, output(b"", b"path is a directory\n", 1)),
            // The script failed without saying why.
            chunk_from_output(0, output(b"", b"", 1)),
        ];

        insta::assert_debug_snapshot!(cases.into_iter().map(render).collect::<Vec<_>>());
    }

    #[test]
    fn test_started_from_output() {
        let output = |stdout: &[u8], stderr: &[u8], exit_code: i32| Output {
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
            exit_code,
        };
        let render = |started: anyhow::Result<bool>| match started {
            Ok(started) => format!("started={started}"),
            Err(err) => format!("error: {err:#}"),
        };

        let cases = [
            // The wrapper recorded the command's process group.
            started_from_output(output(b"started\n", b"", 0)),
            // The probe claimed the exec directory, so the command never runs.
            started_from_output(output(b"unstarted\n", b"", 0)),
            // The wrapper created the directory and then died.
            started_from_output(output(
                b"",
                b"exec directory exists but the command's process group was never recorded\n",
                1,
            )),
            // The script failed without saying why.
            started_from_output(output(b"", b"", 1)),
            // The script said something else.
            started_from_output(output(b"maybe\n", b"", 0)),
        ];

        insta::assert_debug_snapshot!(cases.into_iter().map(render).collect::<Vec<_>>());
    }

    #[test]
    fn test_validate_relative_path() {
        let cases = [
            ".estuary/flow-token",
            "dev",
            "/etc/passwd",
            "../escape",
            "a/../b",
            "a/..b",
            "..a/b",
            "trailing/",
            "",
        ];
        let render = |path: &str| match validate_relative_path(path) {
            Ok(()) => format!("{path:?}: ok"),
            Err(err) => format!("{path:?}: {err}"),
        };

        insta::assert_debug_snapshot!(cases.iter().map(|path| render(path)).collect::<Vec<_>>());
    }

    /// Spawns [`READ_FILE`] with `$HOME` at `home`.
    fn run_read_file(
        home: &std::path::Path,
        path: &str,
        offset: u64,
        limit: u64,
    ) -> std::process::Output {
        std::process::Command::new("bash")
            .args([
                "-c",
                READ_FILE,
                "flow-read-file",
                path,
                &offset.to_string(),
                &limit.to_string(),
            ])
            .env("HOME", home)
            .output()
            .unwrap()
    }

    /// The script must hand back the file's bytes exactly, from wherever the
    /// caller left off and no further than the limit, since a client
    /// reassembles a file from the chunks it is given. Content that is not
    /// text goes through unchanged: the caller decides what to make of it.
    #[test]
    fn test_read_file_script_reads_a_window_of_the_file() {
        let home = tempfile::tempdir().unwrap();
        let content = b"one\ntwo\n\0\xe9\x9b\xaa\n";
        std::fs::write(home.path().join("out"), content).unwrap();

        let render = |offset: u64, limit: u64| {
            let output = run_read_file(home.path(), "out", offset, limit);
            format!(
                "offset={offset} limit={limit} status={:?} stdout={:?}",
                output.status.code(),
                output.stdout
            )
        };

        let cases = [
            // The whole file, and the same read bounded by the limit.
            render(0, 1024),
            render(0, 4),
            // Continuing from a prior read, including one that resumes inside
            // a multi-byte character.
            render(4, 1024),
            render(10, 1024),
            // A read that starts at, or beyond, the end of the file.
            render(u64::try_from(content.len()).unwrap(), 1024),
            render(4096, 1024),
        ];

        insta::assert_debug_snapshot!(cases);
    }

    /// A path that is not a readable file is the script's own outcome rather
    /// than a shell failure: a missing one exits 3, which [`chunk_from_output`]
    /// turns into a chunk that reports the file absent, and a directory exits
    /// 1 with its reason.
    #[test]
    fn test_read_file_script_reports_a_missing_path_and_refuses_a_directory() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("dir")).unwrap();

        let missing = run_read_file(home.path(), ".estuary/exec/01/exit", 0, 1024);
        assert_eq!(missing.status.code(), Some(3));
        assert!(missing.stdout.is_empty());

        let directory = run_read_file(home.path(), "dir", 0, 1024);
        assert_eq!(directory.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&directory.stderr).contains("directory"),
            "{directory:?}"
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_sandbox_records(pool: sqlx::PgPool) {
        // One user can hold multiple sandboxes, each with a distinct identity.
        let first = insert_record(&pool, ALICE, "dev").await.unwrap();
        assert_eq!((first.user_id, first.catalog_name.as_str()), (ALICE, "dev"));
        assert_eq!(first.handle, handle(first.id));
        let second = insert_record(&pool, ALICE, "other").await.unwrap();

        // The record resolves for its owner only, and lists for its owner only.
        assert_eq!(
            fetch(&pool, first.id, ALICE).await.unwrap().unwrap().id,
            first.id
        );
        assert!(fetch(&pool, first.id, BOB).await.unwrap().is_none());
        assert_eq!(list(&pool, ALICE).await.unwrap().len(), 2);
        assert!(list(&pool, BOB).await.unwrap().is_empty());

        // A failed provider deletion must retain the record for a retry.
        let succeed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let response = succeed.clone();
        let router = axum::Router::new().route(
            "/v1/sprites/{name}",
            axum::routing::delete(move || {
                let response = response.clone();
                async move {
                    if response.load(std::sync::atomic::Ordering::SeqCst) {
                        axum::http::StatusCode::NO_CONTENT
                    } else {
                        axum::http::StatusCode::BAD_GATEWAY
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = crate::sprites::Client::with_base_url(
            "token".to_string(),
            url::Url::parse(&format!("http://{addr}")).unwrap(),
        );
        assert!(delete(&client, &pool, &first).await.is_err());
        assert!(fetch(&pool, first.id, ALICE).await.unwrap().is_some());

        // Successful deletion removes the row and frees its catalog name.
        succeed.store(true, std::sync::atomic::Ordering::SeqCst);
        delete(&client, &pool, &first).await.unwrap();
        server.abort();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM internal.sandboxes WHERE id = $1")
                .bind(first.id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(fetch(&pool, first.id, ALICE).await.unwrap().is_none());

        let fresh = insert_record(&pool, ALICE, "dev").await.unwrap();
        assert_ne!(fresh.id, first.id);
        assert_ne!(fresh.handle, first.handle);
        assert_eq!(fresh.catalog_name, first.catalog_name);
        let remaining: std::collections::BTreeSet<models::Id> = list(&pool, ALICE)
            .await
            .unwrap()
            .into_iter()
            .map(|sandbox| sandbox.id)
            .collect();
        assert_eq!(remaining, [fresh.id, second.id].into_iter().collect());
    }

    /// A stand-in for the Sprites exec endpoint. It answers each request with
    /// the next scripted response, each a status and the body deliveries that
    /// follow it, and records the command each request asked to run.
    #[derive(Default)]
    struct MockApi {
        responses: std::collections::VecDeque<(axum::http::StatusCode, Vec<&'static [u8]>)>,
        /// The `cmd` arguments of every request, in order.
        requests: Vec<Vec<String>>,
        inputs: Vec<(Vec<(String, String)>, bytes::Bytes)>,
    }

    /// Serves `mock` on a local port and returns the base URL a client uses to
    /// reach it. The body of a response is streamed one delivery at a time, so
    /// each delivery reaches the client's frame decoder on its own, as a
    /// sprite's writes do.
    async fn serve_mock(mock: std::sync::Arc<std::sync::Mutex<MockApi>>) -> url::Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler =
            move |axum::extract::Query(query): axum::extract::Query<Vec<(String, String)>>,
                  body: bytes::Bytes| {
                let mock = mock.clone();
                async move {
                    let (status, deliveries) = {
                        let mut mock = mock.lock().unwrap();
                        mock.inputs.push((query.clone(), body));
                        mock.requests.push(
                            query
                                .into_iter()
                                .filter(|(key, _)| key == "cmd")
                                .map(|(_, value)| value)
                                .collect(),
                        );
                        mock.responses
                            .pop_front()
                            .expect("the mock has a response left for this request")
                    };
                    let body = axum::body::Body::from_stream(futures::stream::iter(
                        deliveries.into_iter().map(|delivery| {
                            Ok::<_, std::io::Error>(bytes::Bytes::from_static(delivery))
                        }),
                    ));
                    (status, body)
                }
            };
        let router =
            axum::Router::new().route("/v1/sprites/{name}/exec", axum::routing::post(handler));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        url::Url::parse(&format!("http://{addr}")).unwrap()
    }

    #[tokio::test]
    async fn test_stdin_travels_only_in_the_request_body() {
        let input = "invented-credential='secret'\n雪\n";
        let mock = std::sync::Arc::new(std::sync::Mutex::new(MockApi::default()));
        mock.lock()
            .unwrap()
            .responses
            .push_back((axum::http::StatusCode::OK, vec![b"\x01started\n"]));
        let client = crate::sprites::Client::with_base_url(
            "token".to_string(),
            serve_mock(mock.clone()).await,
        );
        let frames = client
            .exec_stream_with_stdin("sandbox", &["cat"], Some(input), START_TIMEOUT)
            .await
            .unwrap();
        drop(frames);
        let mock = mock.lock().unwrap();
        let (query, body) = &mock.inputs[0];
        assert_eq!(body.as_ref(), input.as_bytes());
        insta::assert_debug_snapshot!(query, @r###"
        [
            (
                "cmd",
                "cat",
            ),
            (
                "stdin",
                "true",
            ),
        ]
        "###);
    }

    #[tokio::test]
    async fn test_stdin_is_not_reflected_in_provider_errors() {
        let mock = std::sync::Arc::new(std::sync::Mutex::new(MockApi::default()));
        mock.lock().unwrap().responses.push_back((
            axum::http::StatusCode::BAD_REQUEST,
            vec![br#"{"error":"rejected input: invented-secret"}"#],
        ));
        let client =
            crate::sprites::Client::with_base_url("token".to_string(), serve_mock(mock).await);
        let err = match client
            .exec_stream_with_stdin("sandbox", &["cat"], Some("invented-secret"), START_TIMEOUT)
            .await
        {
            Ok(_) => panic!("provider should reject the request"),
            Err(err) => err,
        };
        assert!(!format!("{err:?}").contains("invented-secret"));
        assert!(format!("{err}").contains("400"));
    }

    /// Lost launches are probed without retrying a potentially running command.
    #[tokio::test]
    async fn test_exec_probes_lost_launches() {
        use axum::http::StatusCode;

        let sandbox = mock_sandbox();
        let mut ids = models::IdGenerator::new(1);
        let mock = std::sync::Arc::new(std::sync::Mutex::new(MockApi::default()));
        let client = crate::sprites::Client::with_base_url(
            "token".to_string(),
            serve_mock(mock.clone()).await,
        );

        let script = |responses: &[(StatusCode, &[&'static [u8]])]| {
            mock.lock().unwrap().responses = responses
                .iter()
                .map(|(status, deliveries)| (*status, deliveries.to_vec()))
                .collect();
        };
        let render = |result: anyhow::Result<ExecEvent>| match result {
            Ok(event) => format!("started {:?}", event.command),
            Err(err) => format!("error: {err:#}"),
        };
        let mut outcomes = Vec::new();

        // The wrapper announces itself: the command is running.
        script(&[(StatusCode::OK, &[b"\x01started\n"])]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo one", None).await,
        ));

        // The API refuses the command outright.
        script(&[(
            StatusCode::BAD_REQUEST,
            &[br#"{"error":"no such sprite state"}"#],
        )]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo two", None).await,
        ));

        // The wrapper could not create the exec directory, and says so before
        // it redirects its stderr.
        script(&[(StatusCode::OK, &[b"\x02mkdir: cannot create directory\n"])]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo three", None).await,
        ));

        // The wrapper exits before announcing anything.
        script(&[(StatusCode::OK, &[b"\x03\x7d"])]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo four", None).await,
        ));

        // The response ends before any frame, so the launch is in doubt. The
        // probe finds no marker and claims the directory: the command never
        // runs.
        script(&[
            (StatusCode::OK, &[]),
            (StatusCode::OK, &[b"\x01unstarted\n", b"\x03\x00"]),
        ]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo five", None).await,
        ));

        // The same lost launch, but the probe finds the marker: the command is
        // running after all.
        script(&[
            (StatusCode::OK, &[]),
            (StatusCode::OK, &[b"\x01started\n", b"\x03\x00"]),
        ]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo six", None).await,
        ));

        // The probe itself fails, so the command may be running. A later
        // listing can discover its metadata in the sandbox.
        script(&[
            (StatusCode::OK, &[]),
            (StatusCode::INTERNAL_SERVER_ERROR, &[br#"{"error":"boom"}"#]),
        ]);
        outcomes.push(render(
            exec(&client, ids.next(), &sandbox, "echo seven", None).await,
        ));

        insta::assert_debug_snapshot!(outcomes);

        // Every request wrapped its script's name and the exec id, and each
        // probe asked after the launch that preceded it.
        let requests = std::mem::take(&mut mock.lock().unwrap().requests);
        let names: Vec<&str> = requests.iter().map(|argv| argv[3].as_str()).collect();
        assert_eq!(
            names,
            [
                "flow-exec",
                "flow-exec",
                "flow-exec",
                "flow-exec",
                "flow-exec",
                "flow-probe-start",
                "flow-exec",
                "flow-probe-start",
                "flow-exec",
                "flow-probe-start",
            ]
        );
        for pair in requests.windows(2) {
            if pair[1][3] == "flow-probe-start" {
                assert_eq!(pair[0][4], pair[1][4], "the probe names the launch's exec");
            }
        }
        assert_eq!(requests[0][2], EXEC_WRAPPER);
        assert_eq!(requests[0][5], "echo one");
        assert_eq!(requests[5][2], PROBE_START);
    }

    /// A stand-in sandbox record with no database behind it, for tests that
    /// exercise the Sprites client alone.
    fn mock_sandbox() -> Sandbox {
        Sandbox {
            id: models::Id::from_hex("0102030405060708").unwrap(),
            user_id: uuid::Uuid::nil(),
            handle: "sandbox".to_string(),
            catalog_name: "acmeCo/test".to_string(),
            created_at: chrono::Utc::now(),
        }
    }
}

/// Exercises the exec scripts against the live Sprites API. Ignored by default:
/// this needs a `SPRITES_TOKEN` for a Fly organization, and it creates and
/// destroys a real VM. Run with:
///
///     cargo test -p control-plane-api sandboxes::live -- --ignored --nocapture
///
/// The scripts are shell, and their behavior belongs to the sprite image's
/// shell rather than to anything this crate can stub, so only a live run covers
/// them.
#[cfg(test)]
mod live {
    use super::*;
    use futures::StreamExt;

    /// Name of the sprite the bootstrap test owns, on the same terms.
    const BOOTSTRAP_SPRITE: &str = "control-plane-api-bootstrap-live-test";

    /// Name of the sprite the launch test owns, on the same terms.
    const LAUNCH_SPRITE: &str = "control-plane-api-launch-live-test";

    /// A record standing in for `handle` without a database behind it. The
    /// scripts reach the sprite by handle alone.
    fn sandbox_for(handle: &str) -> Sandbox {
        Sandbox {
            id: models::Id::from_hex("0102030405060708").unwrap(),
            user_id: uuid::Uuid::nil(),
            handle: handle.to_string(),
            catalog_name: "acmeCo/live-test".to_string(),
            created_at: chrono::Utc::now(),
        }
    }

    /// `launch` drops the connection that started a command, and nothing in
    /// the control plane follows it afterwards, so the command must run to
    /// completion on its own and leave its output and exit status where reads
    /// find them. A client observes completion by polling; and a launch that
    /// never happened is settled by the probe, which blocks a wrapper that
    /// arrives late.
    #[tokio::test]
    #[ignore]
    async fn test_launch_detaches_and_the_command_runs_to_completion() {
        let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN must be set");
        let client = crate::sprites::Client::new(token);

        client.create_sprite(LAUNCH_SPRITE).await.unwrap();
        wait_until_ready(&client, LAUNCH_SPRITE).await.unwrap();
        let sandbox = sandbox_for(LAUNCH_SPRITE);
        // Fresh exec ids each run, so a sprite left behind by an earlier
        // failure cannot answer with that run's exec directories.
        let mut ids = models::IdGenerator::new(1);

        // The command outlasts the launch by seconds, and the launch returns
        // as soon as the wrapper announces it.
        let detached = ids.next();
        let detached_arg = detached.to_string();
        let input = "literal input: '$HOME' $(echo untouched)\n雪\n";
        let input_len = input.len().to_string();
        let argv = [
            "bash",
            "-c",
            EXEC_WRAPPER,
            "flow-exec",
            &detached_arg,
            "for i in 1 2 3; do echo tick $i; sleep 1; done; cat; exit 7",
            &input_len,
            &serde_json::json!({"id": &detached_arg, "command": "for i in 1 2 3; do echo tick $i; sleep 1; done; cat; exit 7", "requested_at": "2026-09-21T00:00:00Z"}).to_string(),
        ];
        let launched = std::time::Instant::now();
        launch(&client, &sandbox, &argv, Some(input)).await.unwrap();
        assert!(
            launched.elapsed() < std::time::Duration::from_secs(3),
            "launch waited on the command: {:?}",
            launched.elapsed()
        );

        // Nothing touches the sandbox while the command runs. It completes on
        // its own, and reads afterwards find everything it wrote and how it
        // exited.
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        let status = read_file(&client, &sandbox, &ExecFile::Exit.path(detached), 0, None)
            .await
            .unwrap();
        assert!(status.exists);
        assert_eq!(String::from_utf8_lossy(&status.bytes).trim(), "7");
        let chunk = read_file(&client, &sandbox, &ExecFile::Stdout.path(detached), 0, None)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&chunk.bytes),
            format!("tick 1\ntick 2\ntick 3\n{input}")
        );

        // The wrapper's marker answers a probe of a started exec.
        assert!(
            probe_started(&client, &sandbox, &detached_arg)
                .await
                .unwrap()
        );

        // A client that wants the result polls with the offsets it is handed,
        // and reads output while the command still runs.
        let polled = ids.next();
        let polled_arg = polled.to_string();
        let argv = [
            "bash",
            "-c",
            EXEC_WRAPPER,
            "flow-exec",
            &polled_arg,
            "echo one; sleep 2; echo two; exit 5",
            "0",
            &serde_json::json!({"id": &polled_arg, "command": "echo one; sleep 2; echo two; exit 5", "requested_at": "2026-09-21T00:00:00Z"}).to_string(),
        ];
        launch(&client, &sandbox, &argv, None).await.unwrap();

        let stdout_path = ExecFile::Stdout.path(polled);
        let exit_path = ExecFile::Exit.path(polled);
        let mut output = Vec::new();
        let mut offset = 0;
        let mut polls = 0;
        let status = loop {
            let chunk = read_file(&client, &sandbox, &stdout_path, offset, None)
                .await
                .unwrap();
            assert_eq!(chunk.offset, offset + chunk.bytes.len() as u64);
            output.extend(chunk.bytes);
            offset = chunk.offset;

            let status = read_file(&client, &sandbox, &exit_path, 0, None)
                .await
                .unwrap();
            polls += 1;
            if status.exists {
                break status;
            }
            assert!(polls < 40, "the command did not finish: {output:?}");
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        };

        // The wrapper writes the exit status after the command's last output,
        // and this client read stdout before reading the status, so one more
        // read collects whatever landed between the two.
        let tail = read_file(&client, &sandbox, &stdout_path, offset, None)
            .await
            .unwrap();
        offset = tail.offset;
        output.extend(tail.bytes);

        assert_eq!(String::from_utf8_lossy(&status.bytes).trim(), "5");
        assert_eq!(String::from_utf8_lossy(&output), "one\ntwo\n");
        assert!(polls > 1, "the command finished before the first poll");
        // A read starting at the end of the file holds its offset and returns
        // nothing, which is how a client sees that it has everything.
        let chunk = read_file(&client, &sandbox, &stdout_path, offset, None)
            .await
            .unwrap();
        assert_eq!(
            (chunk.bytes.len(), chunk.offset, chunk.exists),
            (0, offset, true)
        );

        // A probe of an exec that never launched claims its directory, and a
        // wrapper that arrives afterwards refuses to run the command.
        let late = ids.next();
        let late_arg = late.to_string();
        assert!(!probe_started(&client, &sandbox, &late_arg).await.unwrap());
        let argv = [
            "bash",
            "-c",
            EXEC_WRAPPER,
            "flow-exec",
            &late_arg,
            "touch $HOME/late-command-ran",
            "0",
            &serde_json::json!({"id": &late_arg, "command": "touch $HOME/late-command-ran", "requested_at": "2026-09-21T00:00:00Z"}).to_string(),
        ];
        let output = client
            .exec(LAUNCH_SPRITE, &argv, FILE_READ_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(output.exit_code, 125, "{output:?}");
        let output = client
            .exec(
                LAUNCH_SPRITE,
                &[
                    "bash",
                    "-lc",
                    "test -e $HOME/late-command-ran && echo ran || echo did-not-run",
                ],
                FILE_READ_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "did-not-run"
        );

        assert!(client.delete_sprite(LAUNCH_SPRITE).await.unwrap());
    }

    /// [`bootstrap`] must install flowctl before `sandboxCreate` returns.
    #[tokio::test]
    #[ignore]
    async fn test_bootstrap_installs_flowctl() {
        let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN must be set");
        let client = crate::sprites::Client::new(token);

        provision(&client, BOOTSTRAP_SPRITE).await.unwrap();

        let output = client
            .exec(
                BOOTSTRAP_SPRITE,
                &["bash", "-lc", "flowctl --version"],
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.starts_with("flowctl v"), "unexpected: {stdout}");

        assert!(client.delete_sprite(BOOTSTRAP_SPRITE).await.unwrap());
    }
}
