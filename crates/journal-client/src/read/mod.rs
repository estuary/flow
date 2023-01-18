mod retry;
pub mod uncommitted;

use std::borrow::Cow;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("grpc error: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("read response not OK: {0:?}")]
    NotOk(::proto_gazette::broker::Status),

    #[error("executing fragment fetch request: {0}")]
    FragmentRequestFailed(#[from] ::reqwest::Error),

    #[error("reading fragment file content: {0}")]
    FragmentRead(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    ProtocolError(Cow<'static, str>),
}

fn io_err<T: Into<Error>>(inner: T) -> io::Error {
    io::Error::new(io::ErrorKind::Other, inner.into())
}

macro_rules! async_try {
    ($e:expr) => {
        match $e {
            Ok(t) => t,
            Err(err) => return Poll::Ready(Err(err.into())),
        }
    };
}
pub(crate) use async_try;
