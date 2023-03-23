pub use std::process::Command;

#[cfg(unix)]
use std::os::fd::OwnedFd as OwnedImpl;
#[cfg(windows)]
use std::os::fd::OwnedHandle as OwnedImpl;

pub struct Child {
    inner: std::process::Child,
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
            inner,
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

    pub async fn wait(mut self) -> std::io::Result<std::process::ExitStatus> {
        let handle = tokio::runtime::Handle::current().spawn_blocking(move || self.inner.wait());
        handle.await.expect("wait does not panic")
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.kill_on_drop {
            _ = self.inner.kill()
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
