use crate::errors::Error;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use std::process::{ExitStatus, Stdio};
use tempfile::NamedTempFile;
use tokio::process::{Child, Command};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CommandConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
}

// Start the proxied connector as a process.
pub fn invoke_connector(
    stdin: Stdio,
    stdout: Stdio,
    entrypoint: &str,
    args: &[String],
) -> Result<Child, Error> {
    tracing::info!("invoke connector {}, {:?}", entrypoint, args);

    Command::new(entrypoint)
        .stdin(stdin)
        .stdout(stdout)
        .stderr(Stdio::inherit())
        .args(args)
        .spawn()
        .map_err(|e| e.into())
}

pub fn invoke_connector_wrapper(entrypoint: String, args: Vec<String>) -> Result<Child, Error> {
    tracing::info!("invoke connector_wrapper {}, {:?}", entrypoint, args);

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

    let wrapper_entrypoint = std::env::current_exe()?;
    let wrapper_entrypoint = wrapper_entrypoint.to_str().expect("unexpected binary path");

    let child = invoke_connector(
        Stdio::piped(),
        Stdio::piped(),
        wrapper_entrypoint,
        &vec!["delayed-execute".to_string(), config_file_path.to_string()],
    )?;

    stop_process(child.id())?;
    Ok(child)
}

pub fn stop_process(pid: Option<u32>) -> Result<(), Error> {
    tracing::info!("stopping process delayed process.");
    if let Some(id) = pid {
        signal::kill(
            Pid::from_raw(id.try_into().expect("unexpected negative pid")),
            Signal::SIGSTOP,
        )
        .map_err(|errno| Error::SigcontError(errno))?
    }
    Ok(())
}

pub fn resume_process(pid: u32) -> Result<(), Error> {
    //stop_process(Some(pid))?;
    tracing::info!("resuming delayed process.");
    signal::kill(
        Pid::from_raw(pid.try_into().expect("unexpected negative pid")),
        Signal::SIGCONT,
    )
    .map_err(|errno| Error::SigcontError(errno))
}

// Replace this function after `exit_status_error` is stable. https://github.com/rust-lang/rust/issues/84908
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
                    None => Err(Error::CommandExecutionError(
                        "process terminated by signal".to_string(),
                    )),
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}
