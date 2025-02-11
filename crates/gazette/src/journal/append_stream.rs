use super::Client;
use async_trait::async_trait;
use bytes::BytesMut;
use futures::{Future, Stream, StreamExt};
use proto_gazette::broker::{self, AppendRequest};

const APPEND_BUFFER_LIMIT: usize = 2 ^ 22;

impl Client {
    /// Appends a stream of [`FramedMessage`]s in-order via a sequence of batched append RPCs.
    /// Returns a Stream of [`RetryResult`]s containing one [`broker::AppendResponse`] per
    /// successful append, and any number of [`RetryError`]s. Just like [`journal::Client::append()`],
    /// after getting an `Err` you can continue to poll the stream to retry.
    ///
    /// While `Client::append()` is suitable for one-off appends of a single buffer, `append_stream`
    /// is for continuously appending an ordered stream of messages. Messages are buffered up to
    /// 4MB (APPEND_BUFFER_LIMIT) if an append is already in-flight. If the buffer is full,
    /// backpressure is applied by pausing consumption of the input stream.
    pub fn append_stream<'a, S>(
        &'a self,
        // req is a template request used for the Append RPCs
        req: AppendRequest,
        mut messages: S,
    ) -> impl Stream<Item = crate::RetryResult<broker::AppendResponse>> + 'a
    where
        S: Stream<Item = Box<dyn FramedMessage>> + Unpin + 'a,
    {
        let mut buf = BytesMut::new();

        let resp = coroutines::coroutine(move |mut co| async move {
            let mut attempt = 0;

            loop {
                tokio::select! {
                    // Always start a new append request as soon as possible: either
                    // the previous one finished and there's buffered data, or we
                    // got our first message to send.
                    biased;

                    // Append requests run one at a time, and we always try to start the
                    // next one as soon as we can. "Poll to retry" behavior of `[Client::append()]`
                    // is retained, except now there can be multiple `Ok` responses since
                    // we're chaining together more than 1 append request.
                    _ = async {
                        let append_buf = buf.split().freeze();
                        let append_stream = self.append(req.clone(), || {
                            futures::stream::once({
                                let append_buf = append_buf.clone();
                                async move { Ok(append_buf) }
                            })
                        });
                        tokio::pin!(append_stream);
                        loop {
                            match append_stream.next().await {
                                Some(Ok(response)) => {
                                    () = co.yield_(Ok(response)).await;
                                }
                                Some(Err(e)) => {
                                    () = co.yield_(Err(e)).await;
                                }
                                None => break
                            }
                        }
                    }, if buf.len() > 0 => {}

                    // So long as we have room in our buffer, eagerly read messages from
                    // the input stream and buffer them until they can be sent out with
                    // the next append. If we hit the buffer cap, apply backpressure by
                    // not consuming any more messages.
                    Some(msg) = messages.next(), if buf.len() < APPEND_BUFFER_LIMIT => {
                        match msg.serialize(buf.clone()).await {
                            Ok(new_buf) =>{
                                attempt = 0;
                                buf = new_buf;
                            },
                            Err(e) => {
                                () = co.yield_(Err(crate::RetryError { attempt, inner: crate::Error::AppendRead(e) })).await;
                                attempt += 1;
                            }
                        }
                    },
                }
            }
        });

        resp
    }
}

#[async_trait]
pub trait FramedMessage: Send + Sync {
    async fn serialize(self: Box<Self>, buf: BytesMut) -> std::io::Result<BytesMut>;
}

#[async_trait]
impl<Fut, T> FramedMessage for T
where
    Fut: Future<Output = std::io::Result<BytesMut>> + Send,
    T: FnOnce(BytesMut) -> Fut + Send + Sync + 'static,
{
    async fn serialize(self: Box<Self>, buf: BytesMut) -> std::io::Result<BytesMut> {
        (self)(buf).await
    }
}
