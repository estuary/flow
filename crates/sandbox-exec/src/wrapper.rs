use crate::codec::{Header, Input, Output, PROTOCOL_VERSION};
use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct Config {
    /// Directory under which workspace directories are created.
    pub workspace_root: std::path::PathBuf,
    /// Directory whose contents are copied into each new workspace.
    /// A missing skeleton directory means an empty workspace.
    pub workspace_skeleton: std::path::PathBuf,
    /// How long a cancelled command has to exit after SIGTERM, before SIGKILL.
    pub kill_grace: std::time::Duration,
    /// How long to read remaining stdout and stderr after the command exits.
    pub drain_timeout: std::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            workspace_root: "/work".into(),
            workspace_skeleton: "/opt/estuary/skel".into(),
            kill_grace: std::time::Duration::from_secs(5),
            drain_timeout: std::time::Duration::from_secs(1),
        }
    }
}

// Maximum bytes of a single read of the command's stdout or stderr.
const CHUNK_SIZE: usize = 64 << 10;
// Maximum Stdin payload bytes queued for the command before the wrapper stops
// reading input frames.
const STDIN_QUEUE_BYTES: usize = 8 << 20;

/// Run one command as described by the Header frame of `input`, relaying
/// `input` frames to the command and its output as frames to `output`.
///
/// A raw EOF of `input` is the only teardown signal from the client. Writes of
/// `output` are best effort: once one fails, the command's remaining output is
/// read and discarded.
///
/// Returns Ok after the command exits and an Exit frame is written.
/// Returns an error if the command could not be run (after writing an Error
/// frame), if it was cancelled because `input` closed or `cancel` resolved, or
/// if the Exit frame couldn't be written.
pub async fn run<R, W, C>(
    config: &Config,
    mut input: R,
    mut output: W,
    cancel: C,
) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
    C: std::future::Future<Output = ()>,
{
    let mut cancel = std::pin::pin!(cancel);
    let mut in_buf = bytes::BytesMut::new();

    let first = tokio::select! {
        first = next_input(&mut input, &mut in_buf) => first,
        () = cancel.as_mut() => anyhow::bail!("cancelled before receiving a Header"),
    };
    let header = match first {
        Ok(None) => anyhow::bail!("input closed before a Header was received"),
        Ok(Some(Input::Header(header))) => header,
        Ok(Some(_)) => {
            return fail(&mut output, anyhow::anyhow!("first frame must be a Header")).await;
        }
        Err(err) => return fail(&mut output, err).await,
    };
    tracing::debug!(?header, "received Header");

    let mut child = match spawn(config, &header).await {
        Ok(child) => child,
        Err(err) => return fail(&mut output, err).await,
    };
    let pgid = rustix::process::Pid::from_raw(child.id() as i32).expect("child pid is positive");

    let relayed = relay(
        config,
        &mut child,
        pgid,
        &mut input,
        in_buf,
        &mut output,
        cancel,
    )
    .await;

    let status = match relayed {
        Ok(status) => status,
        Err(err) => {
            terminate(&mut child, pgid, config.kill_grace).await;
            return Err(err);
        }
    };
    tracing::debug!(?status, "command exited");

    use std::os::unix::process::ExitStatusExt;
    let mut buf = bytes::BytesMut::new();
    Output::Exit {
        code: status.code(),
        signal: status.signal(),
    }
    .encode(&mut buf)?;
    output
        .write_all(&buf)
        .await
        .context("failed to write Exit frame")?;
    output.flush().await.context("failed to write Exit frame")?;

    Ok(())
}

/// Check the Header and return the workspace directory it names.
fn workspace_dir(config: &Config, header: &Header) -> anyhow::Result<std::path::PathBuf> {
    if header.version != PROTOCOL_VERSION {
        anyhow::bail!(
            "unsupported protocol version {} (expected {PROTOCOL_VERSION})",
            header.version
        );
    }
    if !is_valid_workspace(&header.workspace) {
        anyhow::bail!(
            "invalid workspace name {:?}: must match {WORKSPACE_PATTERN}",
            header.workspace
        );
    }
    Ok(config.workspace_root.join(&header.workspace))
}

/// The rule of workspace names, as a regular expression.
pub const WORKSPACE_PATTERN: &str = "^[a-z0-9][a-z0-9_-]{0,62}$";

/// Whether `name` matches [`WORKSPACE_PATTERN`]. A valid name is a single,
/// non-hidden path component.
pub fn is_valid_workspace(name: &str) -> bool {
    let bytes = name.as_bytes();
    let Some(first) = bytes.first() else {
        return false;
    };
    bytes.len() <= 63
        && (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
}

async fn spawn(config: &Config, header: &Header) -> anyhow::Result<async_process::Child> {
    use std::os::unix::process::CommandExt;

    let dir = workspace_dir(config, header)?;
    let skeleton = config.workspace_skeleton.clone();
    let workspace = dir.clone();
    tokio::task::spawn_blocking(move || create_workspace(&skeleton, &workspace))
        .await
        .expect("create_workspace doesn't panic")
        .with_context(|| format!("failed to create workspace directory {}", dir.display()))?;

    let mut cmd = std::process::Command::new("bash");
    cmd.arg("-lc")
        .arg(&header.command)
        .current_dir(&dir)
        .envs(&header.env)
        .env("HOME", &dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // Its own process group, so that signalling the group reaches
        // background processes the command started, and not the wrapper.
        .process_group(0);

    let child: async_process::Child = cmd.spawn().context("failed to spawn bash")?.into();
    tracing::debug!(pid = child.id(), dir = %dir.display(), "spawned command");
    Ok(child)
}

/// Create workspace directory `dir` holding a copy of the contents of
/// directory `skeleton`, unless `dir` already exists.
///
/// No command observes a partial copy. The copy is built in a temporary
/// sibling of `dir`, which is renamed to `dir` only if `dir` still doesn't
/// exist (`RENAME_NOREPLACE`). A concurrent wrapper which lost that race
/// removes its copy and uses the winner's.
fn create_workspace(skeleton: &std::path::Path, dir: &std::path::Path) -> anyhow::Result<()> {
    match std::fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => return Ok(()),
        Ok(_) => anyhow::bail!("it exists, and is not a directory"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let root = dir.parent().expect("workspace has a parent");
    std::fs::create_dir_all(root)?;

    // Valid workspace names aren't hidden, so the temporary never collides
    // with a workspace. Its mode is that of `std::fs::create_dir`.
    let building = tempfile::Builder::new()
        .prefix(&format!(".{}.", dir.file_name().unwrap().to_string_lossy()))
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o777))
        .tempdir_in(root)?
        .keep();

    // Whether our copy became the workspace.
    let placed =
        copy_skeleton(skeleton, &building).and_then(|()| {
            match rustix::fs::renameat_with(
                rustix::fs::CWD,
                &building,
                rustix::fs::CWD,
                dir,
                rustix::fs::RenameFlags::NOREPLACE,
            ) {
                Ok(()) => Ok(true),
                Err(rustix::io::Errno::EXIST) => Ok(false),
                Err(err) => Err(std::io::Error::from(err))
                    .context("failed to rename the workspace into place"),
            }
        });

    if !matches!(placed, Ok(true))
        && let Err(err) = remove_tree(&building)
    {
        tracing::warn!(?err, path = %building.display(), "failed to remove a workspace under construction");
    }
    let placed = placed?;
    tracing::debug!(dir = %dir.display(), placed, "created workspace");
    Ok(())
}

/// Copy the contents of directory `skeleton`, if it exists, into empty
/// directory `dst`.
fn copy_skeleton(skeleton: &std::path::Path, dst: &std::path::Path) -> anyhow::Result<()> {
    match std::fs::metadata(skeleton) {
        Ok(_) => copy_tree(skeleton, dst)
            .with_context(|| format!("failed to copy workspace skeleton {}", skeleton.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read workspace skeleton {}", skeleton.display())),
    }
}

/// Recursively copy the directories, regular files, and symlinks of `src`
/// into existing directory `dst`. Copies have the permission bits of their
/// source, and are owned by us. Symlinks are copied as symlinks.
fn copy_tree(src: &std::path::Path, dst: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    for entry in std::fs::read_dir(src).with_context(|| format!("reading {}", src.display()))? {
        let entry = entry.with_context(|| format!("reading {}", src.display()))?;
        let (from, to) = (entry.path(), dst.join(entry.file_name()));
        let meta = std::fs::symlink_metadata(&from)
            .with_context(|| format!("reading {}", from.display()))?;
        let file_type = meta.file_type();

        if file_type.is_symlink() {
            let target =
                std::fs::read_link(&from).with_context(|| format!("reading {}", from.display()))?;
            std::os::unix::fs::symlink(&target, &to)
                .with_context(|| format!("creating symlink {}", to.display()))?;
        } else if file_type.is_dir() {
            std::fs::create_dir(&to).with_context(|| format!("creating {}", to.display()))?;
            copy_tree(&from, &to)?;
            // Applied after its contents are copied, as the mode may deny writes.
            let mode = std::fs::Permissions::from_mode(meta.permissions().mode() & 0o7777);
            std::fs::set_permissions(&to, mode)
                .with_context(|| format!("setting the mode of {}", to.display()))?;
        } else if file_type.is_file() {
            // `std::fs::copy` also copies the permission bits.
            std::fs::copy(&from, &to).with_context(|| format!("copying {}", from.display()))?;
        } else {
            anyhow::bail!(
                "{} is not a directory, regular file, or symlink",
                from.display()
            );
        }
    }
    Ok(())
}

/// Remove directory tree `path`, first granting ourselves write and search
/// permission on each directory, which a copied mode may have denied.
fn remove_tree(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut pending = vec![path.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let mode = std::fs::symlink_metadata(&dir)?.permissions().mode();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode | 0o700))?;

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    std::fs::remove_dir_all(path)
}

/// Relay frames between the client and the command until the command exits,
/// and then drain its output. Returns its exit status, or an error if the
/// command must be cancelled.
async fn relay<R, W, C>(
    config: &Config,
    child: &mut async_process::Child,
    pgid: rustix::process::Pid,
    input: &mut R,
    mut in_buf: bytes::BytesMut,
    output: &mut W,
    mut cancel: std::pin::Pin<&mut C>,
) -> anyhow::Result<std::process::ExitStatus>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
    C: std::future::Future<Output = ()>,
{
    let child_stdin = child.stdin.take().expect("stdin is piped");
    let child_stdout = child.stdout.take().expect("stdout is piped");
    let child_stderr = child.stderr.take().expect("stderr is piped");

    // Stdin payloads are queued to `stdin_writer`, and hold permits of
    // `queue_permits` until written. The queue is bounded so a command that's
    // slow to read can't make us buffer unbounded input.
    let queue_permits = tokio::sync::Semaphore::new(STDIN_QUEUE_BYTES);
    let (stdin_tx, stdin_rx) = tokio::sync::mpsc::unbounded_channel();
    // None after StdinEof, or after the command closed its stdin.
    let mut stdin_tx = Some(stdin_tx);
    let mut stdin_writer = std::pin::pin!(write_stdin(child_stdin, stdin_rx, &queue_permits));
    let mut stdin_writer_done = false;
    // A Stdin payload which awaits room in the queue. While set, we don't read
    // input, so a hang-up isn't observed until the command reads its stdin or
    // exits.
    let mut blocked: Option<bytes::Bytes> = None;

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let mut pump = std::pin::pin!(pump_output(child_stdout, child_stderr, output, stop_rx));
    let mut pump_done = false;

    let status = loop {
        let blocked_len = blocked.as_ref().map_or(0, |data| data.len() as u32);

        tokio::select! {
            status = child.wait() => break status.context("failed to wait for command")?,
            () = cancel.as_mut() => anyhow::bail!("cancelled"),
            result = pump.as_mut(), if !pump_done => {
                // The command closed stdout and stderr, but may still be running.
                () = result?;
                pump_done = true;
            }
            result = stdin_writer.as_mut(), if !stdin_writer_done => {
                () = result.context("failed to write command stdin")?;
                stdin_writer_done = true;
                (stdin_tx, blocked) = (None, None);
            }
            permit = queue_permits.acquire_many(blocked_len), if blocked.is_some() => {
                let permit = permit.expect("semaphore is never closed");
                let data = blocked.take().unwrap();
                if let Some(tx) = &stdin_tx
                    && tx.send(data).is_ok()
                {
                    permit.forget(); // Returned by `write_stdin`.
                }
            }
            frame = next_input(input, &mut in_buf), if blocked.is_none() => {
                let Some(frame) = frame.context("failed to read input")? else {
                    anyhow::bail!("input closed (client hung up)");
                };

                match frame {
                    Input::Header(_) => anyhow::bail!("unexpected Header after the first frame"),
                    Input::StdinEof => stdin_tx = None,
                    // Discarded: the command has closed its stdin (or the
                    // client already sent StdinEof).
                    Input::Stdin(_) if stdin_tx.is_none() => {}
                    Input::Stdin(data) => match queue_permits.try_acquire_many(data.len() as u32) {
                        Ok(permit) => {
                            if stdin_tx.as_ref().unwrap().send(data).is_ok() {
                                permit.forget(); // Returned by `write_stdin`.
                            }
                        }
                        Err(_) => blocked = Some(data),
                    },
                }
            }
        }
    };

    // Kill any background processes the command left behind.
    // They may hold stdout or stderr open, and would otherwise run on.
    signal_group(pgid, rustix::process::Signal::KILL);

    // A process the command detached into its own session (`setsid`) survives
    // the group kill. It may hold stdout or stderr open indefinitely, so the
    // drain is bounded. `stop_tx` stops the pump between frames, never within one.
    if !pump_done {
        tokio::select! {
            result = pump.as_mut() => result?,
            () = tokio::time::sleep(config.drain_timeout) => {
                drop(stop_tx);
                pump.as_mut().await?;
            }
        }
    }

    Ok(status)
}

/// Read and return the next input frame, or None on a clean EOF.
/// Cancel-safe: a partially-read frame remains in `buf`.
async fn next_input<R>(input: &mut R, buf: &mut bytes::BytesMut) -> anyhow::Result<Option<Input>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        if let Some(frame) = Input::decode(buf)? {
            return Ok(Some(frame));
        }
        buf.reserve(CHUNK_SIZE);

        if input.read_buf(buf).await? == 0 {
            if !buf.is_empty() {
                anyhow::bail!("input closed within a partial frame");
            }
            return Ok(None);
        }
    }
}

/// Write queued Stdin payloads to the command, returning their permits to
/// `queue_permits` once written. Returns when the queue's sender is dropped
/// (StdinEof), or when the command has closed its stdin.
async fn write_stdin(
    mut stdin: async_process::ChildStdin,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<bytes::Bytes>,
    queue_permits: &tokio::sync::Semaphore,
) -> std::io::Result<()> {
    while let Some(data) = rx.recv().await {
        let result = stdin.write_all(&data).await;
        queue_permits.add_permits(data.len());

        match result {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(err) => return Err(err),
        }
    }
    Ok(()) // Dropping `stdin` closes it.
}

/// Read the command's stdout and stderr, writing each read as a frame.
/// Writes are best effort: after one fails, reads are discarded.
/// Returns when both are closed, or when `stop` is signalled.
async fn pump_output<W>(
    stdout: async_process::ChildOutput,
    stderr: async_process::ChildOutput,
    output: &mut W,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let (mut stdout, mut stderr) = (Some(stdout), Some(stderr));
    let (mut stdout_chunk, mut stderr_chunk) = (vec![0; CHUNK_SIZE], vec![0; CHUNK_SIZE]);
    let mut buf = bytes::BytesMut::new();
    let mut writable = true;

    while stdout.is_some() || stderr.is_some() {
        let frame = tokio::select! {
            n = read_pipe(&mut stdout, &mut stdout_chunk), if stdout.is_some() => {
                match n.context("failed to read command stdout")? {
                    0 => { stdout = None; continue; }
                    n => Output::Stdout(bytes::Bytes::copy_from_slice(&stdout_chunk[..n])),
                }
            }
            n = read_pipe(&mut stderr, &mut stderr_chunk), if stderr.is_some() => {
                match n.context("failed to read command stderr")? {
                    0 => { stderr = None; continue; }
                    n => Output::Stderr(bytes::Bytes::copy_from_slice(&stderr_chunk[..n])),
                }
            }
            _ = &mut stop => return Ok(()),
        };

        if !writable {
            continue;
        }
        buf.clear();
        frame.encode(&mut buf)?;

        if let Err(err) = async {
            output.write_all(&buf).await?;
            output.flush().await
        }
        .await
        {
            tracing::debug!(
                ?err,
                "failed to write output frame; discarding further output"
            );
            writable = false;
        }
    }
    Ok(())
}

// Reads from `pipe`, which must be Some.
// The check is deferred to the returned future so that a disabled select! branch,
// whose future is built but never polled, doesn't panic.
async fn read_pipe(
    pipe: &mut Option<async_process::ChildOutput>,
    chunk: &mut [u8],
) -> std::io::Result<usize> {
    pipe.as_mut().expect("pipe is open").read(chunk).await
}

/// Cancel the command: SIGTERM its process group, give it `grace` to exit,
/// then SIGKILL the group and reap the command.
async fn terminate(
    child: &mut async_process::Child,
    pgid: rustix::process::Pid,
    grace: std::time::Duration,
) {
    signal_group(pgid, rustix::process::Signal::TERM);

    if tokio::time::timeout(grace, child.wait()).await.is_err() {
        tracing::debug!(
            pid = child.id(),
            "command did not exit within its grace period"
        );
    }
    // Always SIGKILL: background processes may outlive the command's exit.
    signal_group(pgid, rustix::process::Signal::KILL);
    _ = child.wait().await;
}

fn signal_group(pgid: rustix::process::Pid, signal: rustix::process::Signal) {
    match rustix::process::kill_process_group(pgid, signal) {
        // The group has no remaining members.
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(err) => tracing::warn!(?err, ?signal, "failed to signal command process group"),
    }
}

/// Write an Error frame describing `err`, and return `err`.
async fn fail<W>(output: &mut W, err: anyhow::Error) -> anyhow::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = bytes::BytesMut::new();
    Output::Error {
        message: format!("{err:#}"),
    }
    .encode(&mut buf)?;

    // The client may be gone; `err` is what matters to our caller.
    if let Err(write_err) = async {
        output.write_all(&buf).await?;
        output.flush().await
    }
    .await
    {
        tracing::debug!(?write_err, "failed to write Error frame");
    }
    Err(err)
}

#[cfg(test)]
mod test {
    #[test]
    fn test_workspace_names() {
        let names = [
            "a",
            "0",
            "my-ws_1",
            &"a".repeat(63),
            "",
            &"a".repeat(64),
            "-a",
            "_a",
            "A",
            "a.b",
            "a/b",
            "..",
            "é",
        ];
        let valid: Vec<_> = names
            .iter()
            .map(|name| (*name, super::is_valid_workspace(name)))
            .collect();

        insta::assert_debug_snapshot!(valid, @r#"
        [
            (
                "a",
                true,
            ),
            (
                "0",
                true,
            ),
            (
                "my-ws_1",
                true,
            ),
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                true,
            ),
            (
                "",
                false,
            ),
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                false,
            ),
            (
                "-a",
                false,
            ),
            (
                "_a",
                false,
            ),
            (
                "A",
                false,
            ),
            (
                "a.b",
                false,
            ),
            (
                "a/b",
                false,
            ),
            (
                "..",
                false,
            ),
            (
                "é",
                false,
            ),
        ]
        "#);
    }
}
