pub use std::process::{Command, Output, Stdio};
use tokio::io::AsyncReadExt;

use shared_child::SharedChild;
#[cfg(unix)]
use std::os::fd::OwnedFd as OwnedImpl;
#[cfg(windows)]
use std::os::fd::OwnedHandle as OwnedImpl;
use std::sync::Arc;

pub struct Child {
    inner: Arc<SharedChild>,

    pub stdin: Option<ChildStdio>,
    pub stdout: Option<ChildStdio>,
    pub stderr: Option<ChildStdio>,
}

pub type ChildStdio = tokio::fs::File;

impl From<std::process::Child> for Child {
    fn from(mut inner: std::process::Child) -> Self {
        let stdin = map_stdio(inner.stdin.take());
        let stdout = map_stdio(inner.stdout.take());
        let stderr = map_stdio(inner.stderr.take());

        Self {
            inner: Arc::new(SharedChild::new(inner).unwrap()),
            stdin,
            stdout,
            stderr,
        }
    }
}

impl Child {
    pub fn wait(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<std::process::ExitStatus>> {
        let cloned_inner = self.inner.clone();
        let handle = tokio::runtime::Handle::current().spawn_blocking(move || cloned_inner.wait());
        async move { handle.await.expect("wait does not panic") }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Ok(Some(_status)) = self.inner.try_wait() {
            return; // Already exited.
        }
        let pid = self.inner.id();

        #[cfg(unix)]
        {
            use shared_child::unix::SharedChildExt;

            // Note that send_signal() returns Ok() if the child has been waited on.
            if let Err(error) = self.inner.send_signal(libc::SIGTERM) {
                tracing::error!(%pid, ?error, "failed to deliver SIGTERM to child process");
            }
        }

        let wait = self.wait();

        _ = tokio::runtime::Handle::current().spawn(async move {
            // Note that the default docker run --stop-timeout is ten seconds.
            let timeout = tokio::time::sleep(std::time::Duration::from_secs(15));

            tokio::select! {
                exit_code = wait => match exit_code {
                    Err(error) => {
                        tracing::error!(%pid, ?error, "failed to wait for dropped child process");
                    },
                    Ok(exit_code) if !exit_code.success() => {
                        tracing::warn!(%pid, ?exit_code, "dropped child process exited with an error");
                    }
                    Ok(_) => {
                        tracing::debug!(%pid, "dropped child process exited cleanly");
                    }
                },
                _ = timeout => {
                    tracing::error!(%pid, "dropped child process is not exiting");
                }
            };
        });
    }
}

/// Spawn the command and wait for it to exit, buffering its stdout and stderr.
/// Upon its exit return an Output having its stdout, stderr, and ExitStatus.
pub async fn output(cmd: &mut Command) -> std::io::Result<Output> {
    cmd.stdin(Stdio::null());
    cmd.stderr(Stdio::piped());
    cmd.stdout(Stdio::piped());

    let mut child: Child = cmd.spawn()?.into();

    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let (mut stdout_pipe, mut stderr_pipe) =
        (child.stdout.take().unwrap(), child.stderr.take().unwrap());

    let (_, _, wait) = tokio::join!(
        stdout_pipe.read_to_end(&mut stdout),
        stderr_pipe.read_to_end(&mut stderr),
        child.wait(),
    );
    let status = wait?;

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn map_stdio<F>(f: Option<F>) -> Option<ChildStdio>
where
    F: Into<OwnedImpl>,
{
    let f: Option<OwnedImpl> = f.map(Into::into);
    let f: Option<std::fs::File> = f.map(Into::into);
    f.map(Into::into)
}

#[cfg(test)]
mod test {
    use super::{output, Child, Command};

    #[tokio::test]
    async fn test_wait() {
        let child: Child = Command::new("true").spawn().unwrap().into();
        assert!(child.wait().await.unwrap().success());
        let child: Child = Command::new("false").spawn().unwrap().into();
        assert!(!child.wait().await.unwrap().success());
    }

    #[tokio::test]
    async fn test_drop_cancellation() {
        // Sleep for six hours.
        let child: Child = Command::new("sleep").arg("21600").spawn().unwrap().into();
        let wait = child.wait();

        std::mem::drop(child);

        #[cfg(unix)]
        assert_eq!(wait.await.unwrap().to_string(), "signal: 15 (SIGTERM)");
    }

    #[tokio::test]
    async fn test_output() {
        let result = output(Command::new("cat").arg("/this/path/does/not/exist")).await;

        insta::assert_debug_snapshot!(result, @r###"
        Ok(
            Output {
                status: ExitStatus(
                    unix_wait_status(
                        256,
                    ),
                ),
                stdout: "",
                stderr: "cat: /this/path/does/not/exist: No such file or directory\n",
            },
        )
        "###);
    }
}
