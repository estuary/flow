use std::{
    cell::UnsafeCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Start a coroutine using the provide asynchronous function.
/// The function is invoked with a Suspend instance through which
/// the coroutine yields values and receives sent resumption values.
/// Coroutines can be directly driven via calls to start() and then
/// resume(), or if it returns (), it may also be accessed as an
/// instance of futures::Stream.
pub fn coroutine<Fut, Yield, Resume, Done>(
    fut: impl FnOnce(Suspend<Yield, Resume>) -> Fut,
) -> Coroutine<Fut, Yield, Resume>
where
    Fut: Future<Output = Done>,
{
    let mailbox = Arc::new(Mailbox {
        yield_: UnsafeCell::new(None),
        resume: UnsafeCell::new(None),
    });
    let fut = fut(Suspend {
        mailbox: mailbox.clone(),
    });
    Coroutine { mailbox, fut }
}

/// Start a coroutine using the provide asynchronous function,
/// which must return a Result with Ok(()). The function is
/// invoked with a Suspend instance through which the coroutine
/// yields values.
///
/// try_coroutine is well suited for creating a `futures::stream::TryStream`.
/// Unlike `coroutine()`, `try_coroutine()` maps a completion
/// of the Future with a Result::Error into a corresponding
/// TryStream Error item.
pub fn try_coroutine<Fut, Yield, Resume, Error>(
    fut: impl FnOnce(Suspend<Yield, Resume>) -> Fut,
) -> TryCoroutine<Fut, Yield, Resume>
where
    Fut: Future<Output = Result<(), Error>>,
{
    let inner = Arc::new(Mailbox {
        yield_: UnsafeCell::new(None),
        resume: UnsafeCell::new(None),
    });
    let fut = fut(Suspend {
        mailbox: inner.clone(),
    });
    TryCoroutine {
        inner: Coroutine {
            mailbox: inner,
            fut,
        },
    }
}

pin_project_lite::pin_project! {
    /// Coroutine is a Future which acts as an asynchronous coroutine,
    /// yielding values at arbitrary suspension points and resuming
    /// with sent values.
    pub struct Coroutine<Fut, Yield, Resume> {
        mailbox: Arc<Mailbox<Yield, Resume>>,
        #[pin]
        fut: Fut,
    }
}

pin_project_lite::pin_project! {
    /// TryCoroutine is a Coroutine whose Future::Output is a Result,
    /// and maps into a futures::stream::TryStream.
    pub struct TryCoroutine<Fut, Yield, Resume>{
        #[pin]
        inner: Coroutine<Fut, Yield, Resume>
    }
}

/// ResumeResult is the result of a Coroutine resumption,
/// which either yields a value or completes.
pub enum ResumeResult<Yield, Done> {
    Yielded(Yield),
    Done(Done),
}

/// Suspend is passed by-value into a Coroutine Future,
/// and is used to suspend the coroutine by yielding a
/// value and awaiting a received resumption value.
pub struct Suspend<Yield, Resume> {
    mailbox: Arc<Mailbox<Yield, Resume>>,
}

/// Mailbox is shared between a Coroutine, the Suspend passed by-value to its Future.
/// Though contained by an Arc, these are the only two references allowed to exist.
struct Mailbox<Yield, Resume> {
    yield_: UnsafeCell<Option<Yield>>,
    resume: UnsafeCell<Option<Resume>>,
}

// Safety: all references to Mailbox are held within Coroutine,
// either directly or within its owned Future, and are always accessed
// through polling functions that require &mut self.
unsafe impl<Y: Send + Sync, R: Send + Sync> Sync for Mailbox<Y, R> {}

impl<Fut, Yield, Resume, Done> Coroutine<Fut, Yield, Resume>
where
    Fut: Future<Output = Done>,
{
    // Poll the Future of the Coroutine to resume it.
    // If it yields a value or returns, then this poll completes with its Ready(ResumeResult).
    fn poll_resume(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ResumeResult<Yield, Done>> {
        let me = self.project();
        match me.fut.poll(cx) {
            Poll::Pending => {
                // Safety: `me.fut` is the only other reference to the mailbox, and just returned.
                if let Some(value) = unsafe { (&mut *me.mailbox.yield_.get()).take() } {
                    Poll::Ready(ResumeResult::Yielded(value))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(done) => Poll::Ready(ResumeResult::Done(done)),
        }
    }

    /// Start this Coroutine, waiting for it to yield its first value or complete.
    /// NOTE: Calls to start() after the first will never complete.
    /// Use resume() to resume an already-started Coroutine.
    pub async fn start(self: &mut Pin<&mut Self>) -> ResumeResult<Yield, Done> {
        std::future::poll_fn(move |cx| self.as_mut().poll_resume(cx)).await
    }

    /// Send a value to resume this suspended Coroutine, and wait for it to yield
    /// its next value or complete.
    /// NOTE: Calls to resume() an un-started Coroutine will start it,
    /// but the sent value will be Dropped without ever being received by the Coroutine.
    pub async fn resume(self: &mut Pin<&mut Self>, value: Resume) -> ResumeResult<Yield, Done> {
        // Safety: we hold an exclusive &mut Coroutine reference.
        *unsafe { &mut *self.mailbox.resume.get() } = Some(value);
        self.start().await
    }
}

impl<Yield, Resume> Suspend<Yield, Resume> {
    /// Suspend this Coroutine by yielding a value and then waiting to receive
    /// a corresponding resumption value, which is returned.
    pub async fn yield_(&mut self, yielded: Yield) -> Resume {
        {
            // Safety: we are within a polling of the Coroutine future,
            // which is only possible from a &mut reference, and we hold a &mut reference
            // to its inner Suspend instance.
            let cell = unsafe { &mut *self.mailbox.yield_.get() };
            assert!(
                cell.is_none(),
                "yield holds &mut self, so its not possible to call twice without awaiting"
            );
            *cell = Some(yielded);
        }

        std::future::poll_fn(|_| {
            if let Some(value) = unsafe { (&mut *self.mailbox.resume.get()).take() } {
                Poll::Ready(value)
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl<Fut, Yield> futures_core::Stream for Coroutine<Fut, Yield, ()>
where
    Fut: Future<Output = ()>,
{
    type Item = Yield;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let resume = self.mailbox.resume.get();

        match Self::poll_resume(self, cx) {
            Poll::Ready(ResumeResult::Yielded(yielded)) => {
                // Send a resumption now so the Coroutine can be immediately polled again.
                // Safety: we previously held a &mut Coroutine, and just returned
                // from polling it (to which we transferred our &mut reference).
                *unsafe { &mut *resume } = Some(());
                Poll::Ready(Some(yielded))
            }
            Poll::Ready(ResumeResult::Done(())) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// The implementation for TryCoroutine matches a Result::Ok to end-of-stream,
// but a Result::Err becomes a Stream Item instance.
impl<Fut, Ok, Error> futures_core::Stream for TryCoroutine<Fut, Ok, ()>
where
    Fut: Future<Output = Result<(), Error>>,
{
    type Item = Result<Ok, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let resume = self.inner.mailbox.resume.get();

        match Coroutine::<Fut, Ok, ()>::poll_resume(self.project().inner, cx) {
            Poll::Ready(ResumeResult::Yielded(yielded)) => {
                // Send a resumption now so the Coroutine can be immediately polled again.
                // Safety: we previously held a &mut Coroutine, and just returned
                // from polling it (to which we transferred our &mut reference).
                *unsafe { &mut *resume } = Some(());
                Poll::Ready(Some(Ok(yielded)))
            }
            Poll::Ready(ResumeResult::Done(Ok(()))) => Poll::Ready(None),
            Poll::Ready(ResumeResult::Done(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::{StreamExt, TryStreamExt};

    #[tokio::test]
    async fn test_coroutine_yield_and_resume() {
        let mut cr = std::pin::pin!(coroutine(|mut yielder| async move {
            let mut i = 1;
            while let Some(next) = yielder.yield_(i).await {
                i = next;
            }
            i + 1
        }));

        let ResumeResult::Yielded(foo) = cr.start().await else {
            unreachable!()
        };
        assert_eq!(foo, 1);

        let ResumeResult::Yielded(foo) = cr.resume(Some(foo * 2)).await else {
            unreachable!()
        };
        assert_eq!(foo, 2);

        let ResumeResult::Yielded(foo) = cr.resume(Some(foo * 2)).await else {
            unreachable!()
        };
        assert_eq!(foo, 4);

        let ResumeResult::Done(foo) = cr.resume(None).await else {
            unreachable!()
        };
        assert_eq!(foo, 5);
    }

    #[tokio::test]
    async fn test_as_stream() {
        let stream = coroutine(|mut yielder| async move {
            () = yielder.yield_(42).await;
            () = yielder.yield_(52).await;
            () = yielder.yield_(62).await;
        });

        let out = stream.collect::<Vec<_>>().await;
        assert_eq!(out, vec![42, 52, 62]);
    }

    #[tokio::test]
    async fn test_as_try_stream_ok() {
        let stream = try_coroutine(|mut yielder| async move {
            () = yielder.yield_(42).await;
            () = yielder.yield_(52).await;
            () = yielder.yield_(62).await;
            Ok::<_, bool>(())
        });

        let out = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(out, vec![42, 52, 62]);
    }

    #[tokio::test]
    async fn test_as_try_stream_error() {
        let stream = try_coroutine(|mut yielder| async move {
            () = yielder.yield_(42).await;
            return Err(true);
        });

        let out = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(out, true);
    }
}
