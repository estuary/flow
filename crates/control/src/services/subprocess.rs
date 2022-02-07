use async_trait::async_trait;
use tokio::process::Command;

use crate::error::SubprocessError;

#[async_trait]
pub trait Subprocess {
    async fn execute(&mut self) -> Result<String, SubprocessError>;
}

#[async_trait]
impl Subprocess for Command {
    async fn execute(&mut self) -> Result<String, SubprocessError> {
        let output = self.output().await?;
        let status = output.status;

        if status.success() {
            Ok(String::from_utf8(output.stdout)?)
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            Err(SubprocessError::Failure {
                status,
                stdout: stdout.into(),
                stderr: stderr.into(),
            })
        }
    }
}
