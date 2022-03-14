use crate::errors::Error;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::process::{ExitStatus, Stdio};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

const READY: &[u8] = "READY".as_bytes();

// Start the connector directly.
pub fn invoke_connector_direct(
    entrypoint: String,
    args: Vec<String>,
) -> Result<(Child, ChildStdin, ChildStdout, ChildStderr), Error> {
    let child = invoke_connector(
        Stdio::piped(),
        Stdio::piped(),
        Stdio::piped(),
        &entrypoint,
        &args,
    )?;

    parse_child(child)
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

// For storing the entrypoint and args to start a connector.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CommandConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
}
// Instead of starting the connector directly, `invoke_delayed_connector` starts a bouncer process first, which will
// start the real connector after receiving a sigcont signal. Two actions are involved,
// 1. the bouncer process calls the `write_ready` function to inform the parent process that it is ready to receive sigcont signal.
// 2. upon informed, the parent process starts preparing for the connector execution (e.g. creating the input data file),
//    and triggers the bouncer process to start the connector (via the `send_sigcont` function) once preparation is done.
pub async fn invoke_delayed_connector(
    entrypoint: String,
    args: Vec<String>,
) -> Result<(Child, ChildStdin, ChildStdout, ChildStderr), Error> {
    tracing::info!("invoke delayed connector {}, {:?}", entrypoint, args);

    // Saves the configs to start the connector.
    let command_config = CommandConfig {
        entrypoint: entrypoint,
        args: args,
    };
    let config_file = NamedTempFile::new()?;
    serde_json::to_writer(&config_file, &command_config)?;
    let (_, config_file_path) = config_file.keep()?;
    let config_file_path = config_file_path
        .to_str()
        .expect("config file path conversion failed.");

    // Prepares and starts the bouncer process.
    let bouncer_process_entrypoint = std::env::current_exe()?;
    let bouncer_process_entrypoint = bouncer_process_entrypoint
        .to_str()
        .expect("unexpected binary path");

    let child = invoke_connector(
        Stdio::piped(),
        Stdio::piped(),
        Stdio::piped(),
        bouncer_process_entrypoint,
        &vec!["delayed-execute".to_string(), config_file_path.to_string()],
    )?;

    let (child, stdin, stdout, mut stderr) = parse_child(child)?;

    // Waiting for "READY" from the bouncer process.
    let mut ready_buf: Vec<u8> = vec![0; READY.len()];
    match timeout(
        std::time::Duration::from_secs(1),
        stderr.read_exact(&mut ready_buf),
    )
    .await
    {
        Ok(_) => {
            if &ready_buf == READY {
                return Ok((child, stdin, stdout, stderr));
            } else {
                tracing::error!("received unexpected bytes.");
            }
        }
        Err(_) => {
            tracing::error!("timeout: reading from delayed-connector process wrapper.");
        }
    };

    return Err(Error::BouncerProcessStartError);
}

pub fn write_ready() {
    std::io::stderr()
        .write_all(READY)
        .expect("failed writing to stdout");
    std::io::stdout()
        .flush()
        .expect("failed flushing to stdout");
}

pub fn send_sigcont(pid: u32) -> Result<(), Error> {
    tracing::info!("resuming bouncer process.");
    signal::kill(
        Pid::from_raw(pid.try_into().expect("unexpected negative pid")),
        Signal::SIGCONT,
    )
    .map_err(|errno| Error::SigcontError(errno))
}

// A more flexible API for starting the connector.
pub fn invoke_connector(
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
    entrypoint: &str,
    args: &[String],
) -> Result<Child, Error> {
    tracing::info!("invoke connector {}, {:?}", entrypoint, args);

    Command::new(entrypoint)
        .stdin(stdin)
        .stdout(stdout)
        .stderr(stderr)
        .args(args)
        .spawn()
        .map_err(|e| e.into())
}

fn parse_child(mut child: Child) -> Result<(Child, ChildStdin, ChildStdout, ChildStderr), Error> {
    let stdout = child.stdout.take().ok_or(Error::MissingIOPipe)?;
    let stdin = child.stdin.take().ok_or(Error::MissingIOPipe)?;
    let stderr = child.stderr.take().ok_or(Error::MissingIOPipe)?;

    Ok((child, stdin, stdout, stderr))
}
