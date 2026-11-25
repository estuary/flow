//! An in-memory [`Provider`] for tests. It records each call, and can be told
//! to fail or to report its machines gone. Its processes are scripted by the
//! test, through the [`Process`] of each.

use super::provider::{Error, Exec, Output, Provider, State};

pub struct Fake {
    pub kind: &'static str,
    pub inner: std::sync::Mutex<Inner>,
    /// When set, `provision` waits for a notification before it returns.
    pub hold_provision: Option<std::sync::Arc<tokio::sync::Notify>>,
}

#[derive(Default)]
pub struct Inner {
    /// Each call as `(operation, handle)`.
    pub calls: Vec<(&'static str, String)>,
    /// Handles of machines which exist.
    pub machines: std::collections::BTreeSet<String>,
    pub fail_provision: bool,
    pub fail_destroy: bool,
    /// Every machine is reported gone.
    pub gone: bool,
    /// Receives each process which `exec` starts. Without it, `exec` fails.
    pub processes: Option<tokio::sync::mpsc::UnboundedSender<Process>>,
}

/// The test's end of a process started by [`Fake::exec`].
pub struct Process {
    pub argv: Vec<String>,
    /// Bytes written to the process's stdin. It yields None once the stdin
    /// sink is dropped.
    pub stdin: tokio::sync::mpsc::UnboundedReceiver<bytes::Bytes>,
    /// Sends items of the process's output. Its `closed()` resolves once the
    /// output stream is dropped.
    pub output: tokio::sync::mpsc::UnboundedSender<Result<Output, Error>>,
}

impl Fake {
    pub fn new(kind: &'static str) -> Self {
        Self {
            kind,
            inner: Default::default(),
            hold_provision: None,
        }
    }

    pub fn calls(&self) -> Vec<(&'static str, String)> {
        self.inner.lock().unwrap().calls.clone()
    }

    pub fn machines(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .machines
            .iter()
            .cloned()
            .collect()
    }

    /// Returns a receiver of each process which `exec` starts from now on.
    pub fn processes(&self) -> tokio::sync::mpsc::UnboundedReceiver<Process> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.inner.lock().unwrap().processes = Some(tx);
        rx
    }

    fn call(&self, op: &'static str, handle: &str) -> std::sync::MutexGuard<'_, Inner> {
        let mut inner = self.inner.lock().unwrap();
        inner.calls.push((op, handle.to_string()));
        inner
    }
}

#[async_trait::async_trait]
impl Provider for Fake {
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn provision(&self, handle: &str) -> Result<State, Error> {
        if self.call("provision", handle).fail_provision {
            return Err(anyhow::anyhow!("fake provision failure").into());
        }
        if let Some(notify) = &self.hold_provision {
            notify.notified().await;
        }
        self.inner
            .lock()
            .unwrap()
            .machines
            .insert(handle.to_string());

        Ok(serde_json::json!({"fake": self.kind}))
    }

    async fn destroy(&self, handle: &str, _state: &State) -> Result<bool, Error> {
        let mut inner = self.call("destroy", handle);
        if inner.fail_destroy {
            return Err(anyhow::anyhow!("fake destroy failure").into());
        }
        if inner.gone {
            return Err(Error::Gone);
        }
        Ok(inner.machines.remove(handle))
    }

    async fn exec(&self, handle: &str, _state: &State, argv: &[&str]) -> Result<Exec, Error> {
        let inner = self.call("exec", handle);
        if inner.gone || !inner.machines.contains(handle) {
            return Err(Error::Gone);
        }
        let Some(processes) = &inner.processes else {
            return Err(anyhow::anyhow!("the fake provider doesn't run processes").into());
        };

        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::unbounded_channel();
        let (output_tx, output_rx) = tokio::sync::mpsc::unbounded_channel();
        processes
            .send(Process {
                argv: argv.iter().map(|arg| arg.to_string()).collect(),
                stdin: stdin_rx,
                output: output_tx,
            })
            .map_err(|_| anyhow::anyhow!("the test stopped receiving processes"))?;

        let stdin = futures::sink::unfold(stdin_tx, |stdin_tx, data: bytes::Bytes| async move {
            stdin_tx
                .send(data)
                .map_err(|_| anyhow::anyhow!("the process closed its stdin"))?;
            Ok::<_, Error>(stdin_tx)
        });
        let output = futures::stream::unfold(output_rx, |mut output_rx| async move {
            let item = output_rx.recv().await?;
            Some((item, output_rx))
        });

        Ok(Exec {
            stdin: Box::pin(stdin),
            output: Box::pin(output),
        })
    }

    async fn list(&self) -> Result<Vec<String>, Error> {
        Ok(self.machines())
    }
}
