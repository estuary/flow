pub use std::process::Command;

use shared_child::SharedChild;
#[cfg(unix)]
use std::os::fd::OwnedFd as OwnedImpl;
#[cfg(windows)]
use std::os::fd::OwnedHandle as OwnedImpl;
use std::sync::Arc;

pub struct Child {
    inner: Arc<SharedChild>,
    kill_on_drop: bool,

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
            kill_on_drop: false,
            stdin,
            stdout,
            stderr,
        }
    }
}

impl Child {
    pub fn kill_on_drop(&mut self, v: bool) {
        self.kill_on_drop = v;
    }

    pub async fn wait(&self) -> std::io::Result<std::process::ExitStatus> {
        let cloned_inner = self.inner.clone();
        let handle = tokio::runtime::Handle::current().spawn_blocking(move || cloned_inner.wait());
        handle.await.expect("wait does not panic")
    }

    pub fn kill(&self) -> Result<(), std::io::Error> {
        self.inner.kill()
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.kill_on_drop {
            let pid = self.inner.id();
            match self.inner.try_wait() {
                // Child has exited
                Ok(Some(exit_code)) => {
                    tracing::debug!(%pid, ?exit_code, "not killing already-exited dropped child process")
                }
                Ok(None) => {
                    let result = self.inner.kill();
                    tracing::debug!(%pid, ?result, "killing dropped child process")
                }
                Err(err) => {
                    let result = self.inner.kill();
                    tracing::debug!(%pid, ?err, ?result, "error checking status of dropped child process, killing anyway");
                }
            }
        }
    }
}

fn map_stdio<F>(f: Option<F>) -> Option<ChildStdio>
where
    F: Into<OwnedImpl>,
{
    let f: Option<OwnedImpl> = f.map(Into::into);
    let f: Option<std::fs::File> = f.map(Into::into);
    f.map(Into::into)
}
