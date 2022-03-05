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
pub fn invoke_connector(entrypoint: &str, args: &[String]) -> Result<Child, Error> {
    Command::new(entrypoint)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .args(args)
        .spawn()
        .map_err(|e| e.into())
}

pub fn invoke_connector_wrapper(entrypoint: String, args: Vec<String>) -> Result<Child, Error> {
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

    invoke_connector(
        wrapper_entrypoint,
        &vec![
            "--config-file-path".to_string(),
            config_file_path.to_string(),
        ],
    )
}

pub fn resume_process(pid: u32) -> Result<(), Error> {
    signal::kill(
        Pid::from_raw(pid.try_into().expect("unexpected negative pid")),
        Signal::SIGCONT,
    )
    .map_err(|errno| Error::SigcontError(errno))
}

// Replace this function after `exit_status_error` is stable. https://github.com/rust-lang/rust/issues/84908
pub fn check_exit_status(result: std::io::Result<ExitStatus>) -> Result<(), Error> {
    match result {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                match status.code() {
                    Some(code) => Err(Error::CommandExecutionError(format!(
                        "failed with code {}.",
                        code
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
