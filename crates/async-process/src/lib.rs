pub use std::process::{Command, Output, Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use std::os::fd::OwnedFd as OwnedImpl;
#[cfg(windows)]
use std::os::fd::OwnedHandle as OwnedImpl;

pub struct Child {
    pid: libc::pid_t,
    status: Status,

    pub stdin: Option<ChildStdio>,
    pub stdout: Option<ChildStdio>,
    pub stderr: Option<ChildStdio>,
}

/// State machine for reaping the child process.
///
/// A `spawn_blocking` task reaps the child by blocking in its `wait()`.
/// `Status` tracks whether that task is still running, has completed (yielding
/// the reaped `std::process::Child`), or was abandoned because its runtime was
/// torn down before it could reap.
enum Status {
    /// The `spawn_blocking` reaping task is running; it yields the reaped child.
    Reaping(tokio::task::JoinHandle<std::process::Child>),
    /// The child has been reaped and its exit status can be queried.
    Reaped(std::process::Child),
    /// The reaping task did not complete: its runtime was torn down (join was
    /// cancelled) or it panicked. The child was not reaped by us, so `Drop`
    /// must SIGKILL *and* reap it (nobody else will) to avoid a zombie.
    Abandoned,
}

pub type ChildStdio = tokio::fs::File;

impl From<std::process::Child> for Child {
    fn from(mut inner: std::process::Child) -> Self {
        let stdin = map_stdio(inner.stdin.take());
        let stdout = map_stdio(inner.stdout.take());
        let stderr = map_stdio(inner.stderr.take());

        let pid = inner.id() as libc::pid_t;
        let status = Status::Reaping(tokio::runtime::Handle::current().spawn_blocking(move || {
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
    pub fn id(&self) -> u32 {
        self.pid as u32
    }

    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Status::Reaping(handle) = &mut self.status {
            match handle.await {
                Ok(inner) => self.status = Status::Reaped(inner),
                Err(join_err) => {
                    // The reaping `spawn_blocking` task did not complete: its
                    // runtime was torn down (join cancelled) or it panicked.
                    // This is a routine teardown path — e.g. a cancelled shard
                    // context — so surface it as an error rather than panicking.
                    // `Drop` will SIGKILL the (unreaped) child via `self.pid`.
                    self.status = Status::Abandoned;
                    return Err(std::io::Error::other(format!(
                        "child reaping task did not complete: {join_err}"
                    )));
                }
            }
        }
        match &mut self.status {
            Status::Reaped(inner) => inner.wait(),
            // `Reaping` was just resolved above, so this is `Abandoned`.
            _ => Err(std::io::Error::other("child reaping task did not complete")),
        }
    }

    /// Reports whether the child process has been reaped (its exit status
    /// collected). A `false` result means the child may still be running, or
    /// has exited but not yet been reaped.
    pub fn is_finished(&mut self) -> bool {
        self.try_resolve();
        matches!(self.status, Status::Reaped(_))
    }

    // Advance `Reaping` if the reaping task has already terminated: to `Reaped`
    // if it yielded the reaped child, or `Abandoned` if it was cancelled or
    // panicked without reaping. A JoinHandle may be polled outside a runtime,
    // so this is safe to call from `Drop`.
    fn try_resolve(&mut self) {
        let Status::Reaping(handle) = &mut self.status else {
            return;
        };
        match futures::FutureExt::now_or_never(handle) {
            Some(Ok(inner)) => self.status = Status::Reaped(inner),
            Some(Err(_)) => self.status = Status::Abandoned,
            None => {} // Still reaping.
        }
    }

    // SIGKILL the child. It may already have exited (ESRCH), which is fine.
    fn kill(&self) {
        if unsafe { libc::kill(self.pid, libc::SIGKILL) } == -1 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                tracing::error!(pid = self.pid, err = ?err, "failed to SIGKILL dropped child");
            }
        }
    }

    // SIGKILL the child and reap it, so it doesn't linger as a zombie. Used when
    // the reaping task terminated without reaping and nobody else will do so.
    // The child is not reaped by us, so its pid has not yet been recycled and is
    // safe to signal; SIGKILL cannot be caught, so the following `waitpid` blocks
    // only briefly.
    fn kill_and_reap(&self) {
        self.kill();

        let mut status = 0;
        while unsafe { libc::waitpid(self.pid, &mut status, 0) } == -1 {
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue, // Interrupted by a signal; retry.
                Some(libc::ECHILD) => break,   // Already reaped elsewhere; fine.
                _ => {
                    tracing::error!(pid = self.pid, err = ?err, "failed to reap dropped child");
                    break;
                }
            }
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        self.try_resolve();
        match &self.status {
            // Reaped: the pid may already be recycled, so leave it be.
            Status::Reaped(_) => {}
            // The reaping task is still blocked in the child's `wait()` and will
            // reap it once it exits; just ensure the child exits.
            Status::Reaping(_) => self.kill(),
            // The reaping task terminated without reaping; nobody else will.
            Status::Abandoned => self.kill_and_reap(),
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
    use super::{Child, Command, Status, input_output, output};

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

    // Regression test: if the reaping `spawn_blocking` task doesn't complete —
    // because its runtime was torn down (`JoinError::Cancelled`) or it panicked
    // (`JoinError::Panic`) — then `wait()` must surface an error rather than
    // panicking on `.unwrap()`. This was observed on a routine teardown path
    // (a cancelled runtime-next shard context) during a shard-cancellation soak.
    //
    // Reaping-task failure is not reliably reproducible by tearing down a real
    // runtime (a running `spawn_blocking` task is detached, not cancelled), so
    // we drive a real child process with a hand-built reaping handle that fails
    // in each way.
    #[tokio::test]
    async fn test_wait_survives_reaping_task_cancellation() {
        let (pid, handle) = spawn_cancelled_reaper();
        assert_child_wait_is_graceful(pid, handle).await;
    }

    #[tokio::test]
    async fn test_wait_survives_reaping_task_panic() {
        // A reaping task that panics -> JoinError::Panic.
        let inner = Command::new("sleep").arg("21600").spawn().unwrap();
        let pid = inner.id() as libc::pid_t;
        let handle = tokio::task::spawn_blocking(move || -> std::process::Child {
            let _keep_alive = inner; // Dropped (not reaped) as the closure unwinds.
            panic!("reaping task panic");
        });

        assert_child_wait_is_graceful(pid, handle).await;
    }

    // Drive a `Child` whose reaping `handle` fails through `wait()` and assert it
    // errors gracefully, then that `Drop` kills and reaps the still-live child.
    async fn assert_child_wait_is_graceful(
        pid: libc::pid_t,
        handle: tokio::task::JoinHandle<std::process::Child>,
    ) {
        let mut child = build_abandoned_child(pid, handle);

        // `wait()` returns an error rather than panicking, ...
        let result = child.wait().await;
        assert!(result.is_err(), "expected an error, got {result:?}");
        // ... is idempotent on a repeated call, ...
        assert!(child.wait().await.is_err());
        // ... and reports the unreaped child as not finished so `Drop` kills it.
        assert!(!child.is_finished());
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "child should be alive before Drop"
        );

        std::mem::drop(child);

        // `Drop` SIGKILLed *and* reaped the abandoned child, so its pid is gone
        // entirely, with no lingering zombie.
        assert_child_reaped(pid);
    }

    // Regression test for the drop-without-`wait()` path (e.g. the container
    // guard in runtime-next, which relies solely on `Drop` to kill its child).
    // A cancelled reaping task is "finished" but never reaped it; `Drop` must
    // not mistake that for an exited child (which would leak the live process),
    // and must reap it (which would otherwise leak a zombie).
    #[tokio::test]
    async fn test_drop_without_wait_reaps_cancelled_child() {
        let (pid, handle) = spawn_cancelled_reaper();
        // Let the abort take effect: the task becomes finished but never reaped.
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let mut child = build_abandoned_child(pid, handle);
        assert!(!child.is_finished());
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "child should be alive before Drop"
        );

        std::mem::drop(child); // No `wait()` was ever called.

        assert_child_reaped(pid);
    }

    // Spawn a long-lived `sleep` child and a reaping task which holds it but is
    // cancelled (aborted while pending) -> its join fails with
    // `JoinError::Cancelled`, leaving the child unreaped and alive.
    fn spawn_cancelled_reaper() -> (libc::pid_t, tokio::task::JoinHandle<std::process::Child>) {
        let inner = Command::new("sleep").arg("21600").spawn().unwrap();
        let pid = inner.id() as libc::pid_t;
        let handle = tokio::spawn(async move {
            std::future::pending::<()>().await;
            inner // Never reached; keeps the child unreaped and alive.
        });
        handle.abort();
        (pid, handle)
    }

    // Build a `Child` around a real (still-running) process `pid` whose reaping
    // `handle` fails (its join resolves to a `JoinError`) without reaping.
    fn build_abandoned_child(
        pid: libc::pid_t,
        handle: tokio::task::JoinHandle<std::process::Child>,
    ) -> Child {
        Child {
            pid,
            status: Status::Reaping(handle),
            stdin: None,
            stdout: None,
            stderr: None,
        }
    }

    // Assert `pid` no longer exists: killed *and* reaped, leaving no zombie
    // (a zombie would still respond to `kill(pid, 0)` with success).
    fn assert_child_reaped(pid: libc::pid_t) {
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "child pid {pid} still exists after Drop",
        );
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
