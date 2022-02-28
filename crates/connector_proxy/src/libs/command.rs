use crate::errors::Error;

use std::process::{ExitStatus, Stdio};
use tokio::process::{Child, Command};

// Start the proxied connector as a process.
pub fn invoke_connector(entrypoint: String, args: &[String]) -> Result<Child, Error> {
    Command::new(entrypoint)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .args(args)
        .spawn()
        .map_err(|e| e.into())
}

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
