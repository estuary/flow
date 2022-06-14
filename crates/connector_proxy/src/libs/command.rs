use crate::errors::Error;

use serde::{Deserialize, Serialize};
use std::process::{ExitStatus, Stdio};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

pub const READY: &[u8] = "READY\n".as_bytes();

// Start the connector directly.
pub fn invoke_connector_direct(entrypoint: String, args: Vec<String>) -> Result<Child, Error> {
    invoke_connector(
        Stdio::piped(),
        Stdio::piped(),
        Stdio::inherit(),
        &entrypoint,
        &args,
    )
}

// Check the connector execution exit status.
// TODO: replace this function after `exit_status_error` is stable. https://github.com/rust-lang/rust/issues/84908
pub fn check_exit_status(message: &str, result: std::io::Result<ExitStatus>) -> Result<(), Error> {
    match result {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                match status.code() {
                    Some(code) => Err(Error::CommandExecutionError(format!(
                        "{} failed with code {}.",
                        message, code
                    ))),
                    None => Err(Error::CommandExecutionError(format!(
                        "{} process terminated by signal",
                        message
                    ))),
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}

// For storing the entrypoint and args to start a delayed connector.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CommandConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
}
// Instead of starting the connector directly, `invoke_connector_delayed` starts a
// shell process that reads a first line, and then starts the connector. This is to allow
// time for us to write down configuration files for Airbyte connectors before starting them up.
// The stdin passed to delayed connector processes must start with a line that serves as a signal
// for readiness of the configuration files.
pub fn invoke_connector_delayed(entrypoint: String, args: Vec<String>) -> Result<Child, Error> {
    tracing::debug!(%entrypoint, ?args, "invoke_connector_delayed");

    let flat_args = args
        .iter()
        .map(|arg| format!("\"{arg}\""))
        .collect::<Vec<String>>()
        .join(" ");

    invoke_connector(
        Stdio::piped(),
        Stdio::piped(),
        Stdio::inherit(),
        "sh",
        &vec![
            "-c".to_string(),
            format!("read -r connector_proxy_dummy_var && exec {entrypoint} {flat_args}"),
        ],
    )
}

pub async fn read_ready<R: AsyncRead + std::marker::Unpin>(reader: &mut R) -> Result<(), Error> {
    let mut ready_buf: Vec<u8> = vec![0; READY.len()];
    match timeout(
        std::time::Duration::from_secs(1),
        reader.read_exact(&mut ready_buf),
    )
    .await
    {
        Ok(_) => {
            if &ready_buf == READY {
                Ok(())
            } else {
                Err(Error::NotReady("received unexpected bytes."))
            }
        }
        Err(_) => Err(Error::NotReady(
            "timeout: reading from delayed-connector process wrapper.",
        )),
    }
}

// A more flexible API for starting the connector.
pub fn invoke_connector(
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
    entrypoint: &str,
    args: &[String],
) -> Result<Child, Error> {
    tracing::debug!(%entrypoint, ?args, "invoke_connector");

    Command::new(entrypoint)
        .stdin(stdin)
        .stdout(stdout)
        .stderr(stderr)
        .args(args)
        .spawn()
        .map_err(|e| e.into())
}

pub fn parse_child(mut child: Child) -> Result<(Child, ChildStdin, ChildStdout), Error> {
    let stdout = child.stdout.take().ok_or(Error::MissingIOPipe)?;
    let stdin = child.stdin.take().ok_or(Error::MissingIOPipe)?;

    Ok((child, stdin, stdout))
}
