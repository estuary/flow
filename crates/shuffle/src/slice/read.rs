use super::{BoxedRead, Read};
use proto_gazette::broker;
use std::task::Poll;

pin_project_lite::pin_project! {
    /// Thin wrapper around gazette's `ReadLines` that squelches transient errors,
    /// converts non-transient errors to `anyhow::Error` with journal/binding context,
    /// and delegates all accumulation and alignment logic to the inner `ReadLines`.
    struct ReadImpl<S> {
        #[pin]
        inner: gazette::journal::read::ReadLines<S>,
        binding: u32,
    }
}

pub fn new<S>(inner: S, binding: u32) -> BoxedRead
where
    S: futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + Send + 'static,
{
    let read = ReadImpl {
        binding,
        inner: gazette::journal::read::ReadLines::new(inner, 1_000_000),
    };
    Box::pin(read)
}

impl<S> futures::Stream for ReadImpl<S>
where
    S: futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + Send + 'static,
{
    type Item = anyhow::Result<gazette::journal::read::LinesBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let mut inner = this.inner;

        loop {
            let result = inner.as_mut().poll_next(cx);

            if let Poll::Ready(Some(Err(err))) = &result
                && err.inner.is_transient()
            {
                tracing::warn!(
                    binding = *this.binding,
                    journal = %inner.fragment().journal,
                    attempt = err.attempt,
                    "transient journal read error (will retry)"
                );
                continue;
            }

            break match result {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),

                Poll::Ready(Some(Err(gazette::RetryError {
                    attempt: _,
                    inner: gazette::Error::BrokerStatus(broker::Status::JournalNotFound),
                }))) => {
                    // If the journal was deleted, its collection was reset and
                    // we should gracefully stop our read.
                    Poll::Ready(None)
                }

                Poll::Ready(Some(Err(gazette::RetryError {
                    attempt: _,
                    inner: err,
                }))) => {
                    let err = match err {
                        gazette::Error::Grpc(status) => crate::status_to_anyhow(status),
                        err => anyhow::anyhow!(err),
                    }
                    .context(format!(
                        "read of {} (binding {})",
                        inner.fragment().journal,
                        this.binding,
                    ));

                    Poll::Ready(Some(Err(err)))
                }
            };
        }
    }
}

impl<S> futures::stream::FusedStream for ReadImpl<S>
where
    S: futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<S> Read for ReadImpl<S>
where
    S: futures::Stream<Item = gazette::RetryResult<broker::ReadResponse>> + Send + 'static,
{
    fn binding(&self) -> u32 {
        self.binding
    }
    fn fragment(&self) -> &broker::Fragment {
        self.inner.fragment()
    }
    fn write_head(&self) -> i64 {
        self.inner.write_head()
    }
    fn put_back(self: std::pin::Pin<&mut Self>, content: bytes::Bytes) {
        self.project().inner.put_back(content)
    }
}
