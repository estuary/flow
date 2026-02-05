use tokio::sync::mpsc;

mod binding;
mod queue;
mod service;
mod session;
mod slice;

pub use binding::Binding;
pub use service::Service;

fn new_channel<T>() -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
    mpsc::channel::<T>(32)
}

// Map an anyhow::Error into a tonic::Status.
#[inline]
fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::unknown(format!("{err:?}")),
    }
}

// Map a tonic::Status into an anyhow::Error.
#[inline]
fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Unknown => anyhow::anyhow!(status.message().to_owned()),
        _ => anyhow::Error::new(status),
    }
}

// verify is a convenience for building protocol error messages in a standard, structured way.
// You call verify to establish a Verify instance, which is then used to assert expectations
// over protocol requests or responses.
// If an expectation fails, it produces a suitable error message.
fn verify<'p>(
    source: &'static str,
    expect: &'static str,
    peer_endpoint: &'p str,
    peer_index: usize,
) -> Verify<'p> {
    Verify {
        source,
        expect,
        peer_endpoint,
        peer_index,
    }
}

struct Verify<'p> {
    source: &'static str,
    expect: &'static str,
    peer_endpoint: &'p str,
    peer_index: usize,
}

impl<'p> Verify<'p> {
    #[must_use]
    #[inline]
    fn ok<T>(&self, t: tonic::Result<T>) -> anyhow::Result<T> {
        match t {
            Ok(t) => Ok(t),
            Err(status) => Err(self.fail_status(status)),
        }
    }

    #[must_use]
    #[inline]
    fn not_eof<T>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<T> {
        if let Some(t) = t {
            Ok(self.ok(t)?)
        } else {
            Err(self.fail(Option::<()>::None))
        }
    }

    /*
    #[must_use]
    #[inline]
    fn is_eof<T: std::fmt::Debug>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<()> {
        if let Some(t) = t {
            Err(self.fail(t.map_err(status_to_anyhow)?))
        } else {
            Ok(())
        }
    }
    */

    #[must_use]
    #[cold]
    fn fail<T: serde::Serialize>(&self, t: T) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer_endpoint,
            peer_index,
        } = self;

        let mut t = serde_json::to_string(&t).unwrap();
        t.truncate(4096);

        if t == "None" {
            anyhow::format_err!("unexpected {source} EOF (expected {expect})")
        } else {
            anyhow::format_err!(
                "{source} protocol error (expected {expect}) from {peer_endpoint}@{peer_index}: {t}"
            )
        }
    }

    #[must_use]
    #[cold]
    fn fail_status(&self, status: tonic::Status) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer_endpoint,
            peer_index,
        } = self;

        status_to_anyhow(status).context(format!(
            "{source} error (expected {expect}) from {peer_endpoint}@{peer_index}"
        ))
    }
}
