pub use std::process::{Command, Output, Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use std::os::fd::OwnedFd as OwnedImpl;
#[cfg(windows)]
use std::os::fd::OwnedHandle as OwnedImpl;

pub struct Child {
    pid: libc::pid_t,
    status: Result<tokio::task::JoinHandle<std::process::Child>, std::process::Child>,

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

        let pid = inner.id() as libc::pid_t;
        let status = Ok(tokio::runtime::Handle::current().spawn_blocking(move || {
            _ = inner.wait();
            inner
        }));

        Self {
            pid,
            status,
            stdin,
            stdout,
            stderr,
        }
    }
}

impl Child {
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Ok(handle) = &mut self.status {
            self.status = Err(handle.await.unwrap());
        }
        let Err(inner) = &mut self.status else {
            unreachable!()
        };
        inner.wait()
    }

    pub fn is_finished(&mut self) -> bool {
        match &self.status {
            Ok(handle) => handle.is_finished(),
            Err(_inner) => true,
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.is_finished() {
            // Already exited.
        } else if unsafe { libc::kill(self.pid, libc::SIGKILL) } == -1 {
            tracing::error!(
                pid = self.pid,
                err = ?std::io::Error::last_os_error(),
                "failed to send SIGKILL to dropped child process"
            );
        }
    }
}

/// Spawn the command and wait for it to exit, buffering its stdout and stderr.
/// Upon its exit return an Output having its stdout, stderr, and ExitStatus.
pub async fn output(cmd: &mut Command) -> std::io::Result<Output> {
    input_output(cmd, &[]).await
}

/// Span the command and wait for it to exit, passing it the given input and buffering its stdout and stderr.
/// Upon its exit return an Output having its stdout, stderr, and ExitStatus.
#[tracing::instrument(level = "debug", err, skip_all, fields(args=?cmd.get_args().collect::<Vec<_>>()))]
pub async fn input_output(cmd: &mut Command, input: &[u8]) -> std::io::Result<Output> {
    cmd.stdin(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.stdout(Stdio::piped());

    let mut child: Child = cmd.spawn()?.into();

    // Pre-allocate enough stdout to hold all of `input` without a reallocation.
    // This is a security measure, to avoid extra allocations / heap copies if
    // the output contains sensitive data, as is the case with `sops` decryptions.
    let (mut stdout, mut stderr) = (Vec::with_capacity(input.len()), Vec::new());
    let (mut stdin_pipe, mut stdout_pipe, mut stderr_pipe) = (
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
        child.stderr.take().unwrap(),
    );

    let (_, _, _, wait) = tokio::join!(
        // This wrapping future drops `stdin_pipe` once `input` is written (or fails).
        async move { stdin_pipe.write_all(input).await },
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
    use super::{Child, Command, input_output, output};

    #[tokio::test]
    async fn test_wait() {
        let mut child: Child = Command::new("true").spawn().unwrap().into();
        assert!(child.wait().await.unwrap().success());
        let mut child: Child = Command::new("false").spawn().unwrap().into();
        assert!(!child.wait().await.unwrap().success());
    }

    #[tokio::test]
    async fn test_drop_cancellation() {
        // Sleep for six hours.
        let child: Child = Command::new("sleep").arg("21600").spawn().unwrap().into();
        std::mem::drop(child); // Doesn't block for six hours.
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

    #[tokio::test]
    async fn test_input_output() {
        let result = input_output(
            Command::new("cat").arg("/dev/stdin"),
            "Hello, world!".as_bytes(),
        )
        .await;

        insta::assert_debug_snapshot!(result, @r###"
        Ok(
            Output {
                status: ExitStatus(
                    unix_wait_status(
                        0,
                    ),
                ),
                stdout: "Hello, world!",
                stderr: "",
            },
        )
        "###);
    }
}
