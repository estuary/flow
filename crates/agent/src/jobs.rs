use super::logs;
use futures::TryFutureExt;
use sqlx::types::Uuid;
use tokio::io::AsyncRead;
use tracing::debug;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("job {name:?} with exec {exec:?} encountered an error")]
    NameDetail {
        name: String,
        exec: std::ffi::OsString,
        #[source]
        err: Box<Error>,
    },
    #[error("failed to spawn")]
    Spawn(#[source] std::io::Error),
    #[error("failed to service stdin")]
    Stdin(#[source] std::io::Error),
    #[error("failed to service stdout")]
    Stdout(#[source] std::io::Error),
    #[error("failed to service stderr")]
    Stderr(#[source] std::io::Error),
    #[error("error while waiting for the process")]
    Wait(#[source] std::io::Error),
}

impl Error {
    fn detail(err: Self, name: &str, cmd: &async_process::Command) -> Self {
        Self::NameDetail {
            name: name.to_string(),
            exec: cmd.get_program().to_os_string(),
            err: err.into(),
        }
    }
}

/// run spawns the provided Command, capturing its stdout and stderr
/// into the provided logs_tx identified by |logs_token|. It clears the environment variables of
/// the child process, leaving only `PATH` and `DOCKER_*` variables.
#[tracing::instrument(err, skip(logs_tx, cmd))]
pub async fn run(
    name: &str,
    logs_tx: &logs::Tx,
    logs_token: Uuid,
    cmd: &mut async_process::Command,
) -> Result<std::process::ExitStatus, Error> {
    // Pass through PATH and any docker-related variables, but remove all other environment variables.
    cmd.env_clear().envs(std::env::vars().filter(|&(ref k, _)| {
        k == "PATH" || k.contains("DOCKER") || k == "GOOGLE_APPLICATION_CREDENTIALS"
    }));

    run_without_removing_env(name, logs_tx, logs_token, cmd).await
}

/// Does the same thing as `run`, but doesn't modify the environment given in `cmd`.
#[tracing::instrument(err, level = "debug", skip(logs_tx, cmd))]
pub async fn run_without_removing_env(
    name: &str,
    logs_tx: &logs::Tx,
    logs_token: Uuid,
    cmd: &mut async_process::Command,
) -> Result<std::process::ExitStatus, Error> {
    let child = spawn(name, cmd)?;
    let stdin: &[u8] = &[];

    wait(name, logs_tx, logs_token, stdin, child)
        .await
        .map_err(|err| Error::detail(err, name, cmd))
}

/// spawn a command with the provided job name, returning its created Child.
fn spawn(name: &str, cmd: &mut async_process::Command) -> Result<async_process::Child, Error> {
    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    debug!(program = ?cmd.get_program(), args = ?cmd.get_args().collect::<Vec<_>>(), "invoking");

    cmd.spawn()
        .map_err(Error::Spawn)
        .map_err(|err| Error::detail(err, name, cmd))
        .map(Into::into)
}

/// Wait for the child to exit while servicing its IO.
/// The caller may have already taken any of its stdin, stdout, or stderr handles.
/// If they haven't, then:
///  - stdin is copied to the child's stdin handle.
///  - stdout lines are captured under the given logs_token.
///  - stderr lines are captured under the given logs_token.
///
/// This is a lower-level API. Prefer the run_* variants.
pub async fn wait<I>(
    name: &str,
    logs_tx: &logs::Tx,
    logs_token: Uuid,
    mut stdin: I,
    mut child: async_process::Child,
) -> Result<std::process::ExitStatus, Error>
where
    I: AsyncRead + Unpin,
{
    let c_stdin = child.stdin.take();
    let c_stdout = child.stdout.take();
    let c_stderr = child.stderr.take();

    let stdin = async move {
        if let Some(mut writer) = c_stdin {
            tokio::io::copy(&mut stdin, &mut writer).await?;
        }
        Ok(())
    }
    .map_err(Error::Stdin);

    let stdout = async move {
        if let Some(reader) = c_stdout {
            logs::capture_lines(logs_tx.clone(), format!("{}:1", name), logs_token, reader).await?;
        }
        Ok(())
    }
    .map_err(Error::Stdout);

    let stderr = async move {
        if let Some(reader) = c_stderr {
            logs::capture_lines(logs_tx.clone(), format!("{}:2", name), logs_token, reader).await?;
        }
        Ok(())
    }
    .map_err(Error::Stderr);

    let wait = child.wait().map_err(Error::Wait);

    let (_, _, _, status) = futures::try_join!(stdin, stdout, stderr, wait)?;

    Ok(status)
}

#[allow(dead_code)]
async fn read_stdout(mut reader: async_process::ChildStdio) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    let _ = tokio::io::copy(&mut reader, &mut buffer)
        .await
        .map_err(Error::Stdout)?;
    Ok(buffer)
}
