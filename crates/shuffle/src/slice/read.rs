use futures::stream;
use gazette::broker;
use std::task::Poll;

pin_project_lite::pin_project! {
    pub struct Read {
        pub binding: u32,
        pub journal: String,
        pub write_head: i64,

        pending: Option<Box<Vec<Option<gazette::RetryResult<broker::ReadResponse>>>>>,

        #[pin]
        inner: gazette::journal::read::ReadLines<
            stream::BoxStream<'static, gazette::RetryResult<broker::ReadResponse>>,
        >,
    }
}

impl Read {
    pub fn push_pending(&mut self, response: Option<gazette::RetryResult<broker::ReadResponse>>) {
        let pending = self.pending.get_or_insert_default();
        pending.push(response);
    }

    pub fn pop_pending(&mut self) -> Option<Option<gazette::RetryResult<broker::ReadResponse>>> {
        let pending = match &mut self.pending {
            Some(pending) => pending,
            None => return None,
        };

        let response = pending.pop();
        if pending.is_empty() {
            self.pending = None;
        }
        response
    }
}

impl futures::Stream for Read {
    type Item = anyhow::Result<(i64, Vec<u8>)>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        let mut content = Vec::with_capacity(1024 * 1024);
        let mut offset = i64::MAX;

        // Loop to optimistically accumulate data until we hit TARGET size,
        // or reach one of several stopping conditions.
        // Note that `inner` / ReadLines is FusedStream.
        let done = loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => break false,
                Poll::Ready(None) => break true,
                Poll::Ready(Some(Ok(response))) => match response {
                    broker::ReadResponse {
                        fragment: Some(broker::Fragment { begin, end: _, .. }),
                        write_head,
                        ..
                    } => {
                        *this.write_head = write_head;

                        if !content.is_empty() && begin as usize != offset as usize + content.len()
                        {
                            break false; // Jump in offset.
                        }
                    }
                    broker::ReadResponse {
                        content: this_content,
                        offset: this_offset,
                        ..
                    } => {
                        offset = offset.min(this_offset);
                        content.extend_from_slice(&this_content);

                        if content.len() > TARGET {
                            break false; // Reached target size.
                        }
                    }
                },
                Poll::Ready(Some(Err(gazette::RetryError {
                    attempt,
                    inner: err,
                }))) => match err {
                    err if err.is_transient() || !content.is_empty() => {
                        if attempt != 0 {
                            tracing::warn!(
                                binding=this.binding,
                                journal=%this.journal,
                                attempt,
                                ?err,
                                "journal read error (will retry)",
                            );
                        }
                        continue; // Poll again to schedule retry.
                    }
                    gazette::Error::BrokerStatus(broker::Status::JournalNotFound) => {
                        tracing::warn!(
                            binding=this.binding,
                            journal=%this.journal,
                            "journal was removed",
                        );
                        break true; // Treat as EOF.
                    }
                    gazette::Error::Grpc(status) => {
                        return Poll::Ready(Some(Err(crate::status_to_anyhow(status))));
                    }
                    err => return Poll::Ready(Some(Err(anyhow::Error::new(err)))),
                },
            }
        };

        if content.is_empty() {
            if done {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        } else {
            Poll::Ready(Some(Ok((offset, content))))
        }
    }
}
